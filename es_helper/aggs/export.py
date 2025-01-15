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
    # 构建聚合查询
    aggs_query = {
        "aggs": {
            "field_terms": {
                "terms": {
                    "field": field,
                    "size": size,
                    "show_term_doc_count_error": True,
                    "shard_size": size * 2,  # 设置更大的shard_size以确保准确性
                    "collect_mode": "breadth_first"  # 使用广度优先收集模式
                }
            }
        },
        "size": 0,  # 不需要返回文档
        "track_total_hits": False  # 不跟踪总命中数以提高性能
    }
    
    start_time = time.time()
    progress_thread = None
    
    try:
        # 开始显示进度
        sys.stdout.write("\r正在聚合... 已耗时：0:00:00")
        sys.stdout.flush()
        
        try:
            # 执行聚合查询，增加超时设置
            while True:
                show_progress(start_time)
                response = es_client.search(
                    index=index, 
                    body=aggs_query,
                    request_timeout=3600,  # 设置1小时超时
                    preference='_local'  # 优先从本地分片获取数据
                )
                break
                
        except Exception as e:
            # 如果失败，尝试使用更保守的设置重试
            sys.stdout.write("\n第一次尝试失败，使用更保守的设置重试...\n")
            aggs_query["aggs"]["field_terms"]["terms"]["collect_mode"] = "depth_first"
            while True:
                show_progress(start_time)
                response = es_client.search(
                    index=index, 
                    body=aggs_query,
                    request_timeout=7200,  # 设置2小时超时
                    preference='_local'
                )
                break
        
        # 完成后清除进度显示
        elapsed = time.time() - start_time
        elapsed_str = str(datetime.timedelta(seconds=int(elapsed)))
        sys.stdout.write(f"\r聚合完成！总耗时：{elapsed_str}\n")
        sys.stdout.flush()
        
        # 获取聚合结果
        aggs_result = response['aggregations']['field_terms']
        
        # 检查聚合结果的准确性
        if aggs_result['doc_count_error_upper_bound'] != 0 or aggs_result['sum_other_doc_count'] != 0:
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