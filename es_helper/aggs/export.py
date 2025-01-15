import os
import csv
import datetime
import time
import sys
from elasticsearch import Elasticsearch


def show_progress(start_time):
    """显示进度和耗时"""
    elapsed = time.time() - start_time
    elapsed_str = str(datetime.timedelta(seconds=int(elapsed)))
    sys.stdout.write(f"\r正在聚合... 已耗时：{elapsed_str}")
    sys.stdout.flush()


def export_aggs_to_csv(es_client: Elasticsearch, index: str, field: str, size: int = 10000):
    """
    导出字段聚合结果到CSV文件
    
    Args:
        es_client: Elasticsearch客户端
        index: 索引名称
        field: 聚合字段
        size: 聚合桶的大小
    
    Returns:
        tuple: (导出文件路径, 聚合结果)
    """
    # ES的最大桶限制
    MAX_BUCKETS = 655360
    min_size = 1000  # 最小聚合桶大小

    # 检查并调整size
    if size > MAX_BUCKETS:
        sys.stdout.write(f"警告: 请求的聚合桶大小({size})超过ES限制({MAX_BUCKETS})，已自动调整\n")
        size = MAX_BUCKETS

    def try_aggregation(current_size, is_conservative=False):
        """尝试执行聚合查询"""
        # 确保不会超过ES限制
        current_size = min(current_size, MAX_BUCKETS)

        aggs_query = {
            "aggs": {
                "field_terms": {
                    "terms": {
                        "field": field,
                        "size": current_size,
                        "show_term_doc_count_error": True,
                        "shard_size": min(current_size * 2, MAX_BUCKETS),  # 确保shard_size也不超限
                        "collect_mode": "depth_first" if is_conservative else "breadth_first"
                    }
                }
            },
            "size": 0,
            "track_total_hits": False
        }

        response = es_client.search(
            index=index,
            body=aggs_query,
            request_timeout=7200 if is_conservative else 3600,
            preference='_local'
        )
        return response['aggregations']['field_terms']

    start_time = time.time()
    current_size = size

    try:
        # 开始显示进度
        sys.stdout.write("\r正在聚合... 已耗时：0:00:00")
        sys.stdout.flush()

        # 第一次尝试：使用完整设置
        try:
            while True:
                show_progress(start_time)
                aggs_result = try_aggregation(current_size)

                # 检查结果完整性
                if aggs_result['doc_count_error_upper_bound'] != 0 or (
                        aggs_result['sum_other_doc_count'] != 0 and size < MAX_BUCKETS):
                    raise ValueError("结果不完整，尝试保守策略")
                else:
                    break

        except Exception as e:
            sys.stdout.write("\n第一次尝试失败，切换到保守策略...\n")

            # 保守策略：逐步减小size直到成功
            while current_size >= min_size:
                try:
                    show_progress(start_time)
                    aggs_result = try_aggregation(current_size, True)

                    # 检查doc_count_error_upper_bound
                    if aggs_result['doc_count_error_upper_bound'] != 0:
                        raise ValueError(
                            f"doc_count_error_upper_bound不为0 (值为{aggs_result['doc_count_error_upper_bound']})"
                        )

                    # 如果到这里，说明至少doc_count_error_upper_bound为0，可以接受结果
                    break

                except Exception as inner_e:
                    if "too_many_buckets_exception" in str(inner_e):
                        current_size = min(int(current_size * 0.8), MAX_BUCKETS)  # 确保不超过最大限制
                    else:
                        current_size = int(current_size * 0.8)  # 每次减少20%

                    sys.stdout.write(f"\n减小聚合桶大小到 {current_size}，重试中...\n")
                    if current_size < min_size:
                        raise ValueError(f"即使将size减小到{current_size}仍然失败，请检查字段或集群状态")

        # 完成后清除进度显示
        elapsed = time.time() - start_time
        elapsed_str = str(datetime.timedelta(seconds=int(elapsed)))
        sys.stdout.write(f"\r聚合完成！总耗时：{elapsed_str}\n")
        sys.stdout.flush()

        # 检查聚合结果的准确性
        if aggs_result['doc_count_error_upper_bound'] != 0 or (
                aggs_result['sum_other_doc_count'] != 0 and size < MAX_BUCKETS):
            raise ValueError(
                f"聚合结果不完整: doc_count_error_upper_bound={aggs_result['doc_count_error_upper_bound']}, "
                f"sum_other_doc_count={aggs_result['sum_other_doc_count']}"
            )

        # 获取集群信息
        cluster_info = es_client.info()
        cluster_name = cluster_info['cluster_name']
        cluster_uuid = cluster_info['cluster_uuid']

        # 构建保存路径
        base_dir = os.path.expanduser("~/ElasticSearchHelper/export")
        cluster_dir = os.path.join(base_dir, f"{cluster_name}_{cluster_uuid}")
        index_dir = os.path.join(cluster_dir, index)
        aggs_dir = os.path.join(index_dir, "aggs")
        field_dir = os.path.join(aggs_dir, field)

        # 创建目录结构
        os.makedirs(field_dir, exist_ok=True)

        # 生成文件名
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        csv_filename = f"aggs_{timestamp}.csv"
        csv_path = os.path.join(field_dir, csv_filename)

        # 计算总文档数
        total_docs = sum(bucket['doc_count'] for bucket in aggs_result['buckets'])
        unique_values = len(aggs_result['buckets'])

        # 写入CSV文件
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 写入统计信息
            writer.writerow(['统计信息'])
            writer.writerow(['总文档数', total_docs])
            writer.writerow(['唯一值数量', unique_values])
            writer.writerow(['聚合桶大小', current_size])
            writer.writerow(['原始请求桶大小', size])
            writer.writerow(['doc_count_error_upper_bound', aggs_result['doc_count_error_upper_bound']])
            writer.writerow(['sum_other_doc_count', aggs_result['sum_other_doc_count']])
            writer.writerow([])  # 空行分隔

            # 写入详细数据
            writer.writerow(['详细数据'])
            writer.writerow(['Term', 'Doc Count'])  # 写入表头

            # 写入聚合结果
            for bucket in aggs_result['buckets']:
                writer.writerow([bucket['key'], bucket['doc_count']])

        return csv_path, aggs_result

    except Exception as e:
        # 发生错误时清除进度显示
        sys.stdout.write("\n")
        sys.stdout.flush()
        raise e
