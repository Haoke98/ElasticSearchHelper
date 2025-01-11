import csv
import logging
import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan, BulkIndexError
import os

error_reason_map = {}


def load_field_mapping(mapping_file):
    """
    加载字段映射关系
    """
    mapping = {}
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            source_field = row['source_field']
            target_field = row['target_field']
            mapping[source_field] = target_field
    return mapping


def get_nested_value(source, field_path):
    """
    获取嵌套字段的值
    
    Args:
        source: 源文档
        field_path: 字段路径，如 "jobInfoData.TotalNum"
    """
    current = source
    for part in field_path.split('.'):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def transform_doc(doc, field_mapping, strict_mode=False):
    """
    根据映射关系转换文档
    
    Args:
        doc: 源文档
        field_mapping: 字段映射关系
        strict_mode: 是否启用严格模式
            - True: 只映射在映射表中定义的字段
            - False: 未在映射表中定义的字段按规则处理
    """
    transformed = {}
    source = doc['_source']

    # 首先处理映射表中的字段
    for src_field, target_field in field_mapping.items():
        if '.' in src_field:
            # 处理嵌套字段
            value = get_nested_value(source, src_field)
            if value is not None and target_field:
                transformed[target_field] = value
                pass
        else:
            # 处理普通字段
            if src_field in source and target_field:
                transformed[target_field] = source[src_field]

    # 非严格模式下，处理未映射的字段
    if not strict_mode:
        for src_field, value in source.items():
            # 如果字段不在映射表中，使用原字段名
            if src_field not in field_mapping:
                transformed[src_field] = value

    
    # print("*" * 140)
    
    return transformed


def custom_reindex(source_index, target_index, mapping_file, batch_size=1000, strict_mode=False):
    """
    执行自定义reindex操作
    
    Args:
        source_index: 源索引名称
        target_index: 目标索引名称
        mapping_file: 字段映射文件路径
        batch_size: 批处理大小
        strict_mode: 是否启用严格模式
            - True: 只映射在映射表中定义的字段
            - False: 未在映射表中定义的字段直接复制
    """
    # 获取ES连接
    esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                          http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                          ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)

    # 加载字段映射
    field_mapping = load_field_mapping(mapping_file)

    # 修改处理文档的方式
    action_buffer = []
    total_processed = 0
    success, failed = 0, 0

    try:
        # 获取总文档数
        total_docs = esCli.count(index=source_index)['count']

        # 使用scan遍历文档
        for doc in scan(esCli, index=source_index, scroll='5m', size=batch_size):
            transformed_doc = transform_doc(doc, field_mapping, strict_mode)
            action_buffer.append({
                '_index': target_index,
                '_id': doc['_id'],
                '_source': transformed_doc
            })

            # 当缓冲区达到batch_size时执行批量操作
            if len(action_buffer) >= batch_size:
                # 执行批量操作
                success_batch, failed_batch = bulk_operation(esCli, action_buffer)
                success += success_batch
                failed += failed_batch
                total_processed += len(action_buffer)

                print(f"\r进度：{total_processed}/{total_docs} "
                      f"({(total_processed / total_docs * 100):.2f}%) "
                      f"成功：{success}, 失败：{failed}", end='')

                action_buffer = []  # 清空缓冲区

        # 处理剩余的文档
        if action_buffer:
            success_batch, failed_batch = bulk_operation(esCli, action_buffer)
            success += success_batch
            failed += failed_batch
            total_processed += len(action_buffer)

        print(f"\n重建索引完成: 成功 {success} 条, 失败 {failed} 条")

    except Exception as e:
        logging.error(f"Exception!:{e}", exc_info=True)


def bulk_operation(client, actions):
    """执行批量操作并返回成功失败数"""
    success = failed = 0
    try:
        _success, errors = bulk(client, actions, stats_only=False)
        success += _success
        failed += len(errors)
    except BulkIndexError as e:
        print("BulkIndexErrors:")
        for i, err in enumerate(e.errors, 1):
            opt_dict = err['index']
            status = opt_dict['status']
            err_dict = opt_dict['error']
            err_type = err_dict['type']
            err_reason = err_dict['reason']
            if err_type == 'strict_dynamic_mapping_exception':
                if not error_reason_map.__contains__(err_reason):
                    print(" " * 10, f"{i:3d} {status}", opt_dict['_id'], err_dict)
                    error_reason_map[err_reason] = err
                pass
            else:
                print(" " * 10, f"{i:3d} {status}", opt_dict['_id'], err_dict)
        failed += len(e.errors)
    return success, failed
