import csv
import logging
import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan, BulkIndexError
import os


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


def transform_doc(doc, field_mapping, strict_mode=False):
    """
    根据映射关系转换文档
    
    Args:
        doc: 源文档
        field_mapping: 字段映射关系
        strict_mode: 是否启用严格模式
            - True: 只映射在映射表中定义的字段
            - False: 未在映射表中定义的字段直接复制
    """
    transformed = {}
    source = doc['_source']
    for src_field, value in source.items():
        if src_field in field_mapping:
            # 字段在映射表中
            target_field = field_mapping[src_field]
            if target_field:  # 只有当目标字段不为空时才进行映射
                transformed[target_field] = value
        elif not strict_mode:
            # 非严格模式下，未映射的字段直接复制
            target_field = field_mapping.get(src_field, None)
            if target_field:
                # print(str(src_field).ljust(32, " "), "==>", target_field)
                transformed[target_field] = value
            else:
                # print(str(src_field).ljust(32, " "), "==>", src_field)
                transformed[src_field] = value
            if src_field in ["mainTypeData", "firmTypeStr", "firmTypeData", "mainTypeStr", "firmIndustryDetailInfo"]:
                print(str(src_field).ljust(32, " "), "==>", target_field)
                sys.exit(1)

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
    docs_buffer = []
    total_processed = 0
    success, failed = 0, 0

    try:
        # 获取总文档数
        total_docs = esCli.count(index=source_index)['count']

        # 使用scan遍历文档
        for doc in scan(esCli, index=source_index, scroll='5m', size=batch_size):
            transformed_doc = transform_doc(doc, field_mapping, strict_mode)
            docs_buffer.append({
                '_index': target_index,
                '_id': doc['_id'],
                '_source': transformed_doc
            })

            # 当缓冲区达到batch_size时执行批量操作
            if len(docs_buffer) >= batch_size:
                # 执行批量操作
                success_batch, failed_batch = bulk_operation(esCli, docs_buffer)
                success += success_batch
                failed += failed_batch
                total_processed += len(docs_buffer)

                print(f"\r进度：{total_processed}/{total_docs} "
                      f"({(total_processed / total_docs * 100):.2f}%) "
                      f"成功：{success}, 失败：{failed}", end='')

                docs_buffer = []  # 清空缓冲区

        # 处理剩余的文档
        if docs_buffer:
            success_batch, failed_batch = bulk_operation(esCli, docs_buffer)
            success += success_batch
            failed += failed_batch
            total_processed += len(docs_buffer)

        print(f"\n重建索引完成: 成功 {success} 条, 失败 {failed} 条")

    except Exception as e:
        logging.error(f"Exception!:{e}", exc_info=True)


def bulk_operation(client, docs):
    """执行批量操作并返回成功失败数"""
    success = failed = 0
    try:
        results = bulk(client, docs, stats_only=False)
        for ok, item in results:
            if ok:
                success += 1
            else:
                failed += 1
                print(f"处理文档失败: {item}")
    except BulkIndexError as e:
        logging.error(f"BulkIndexError!")
        print("Errors:")
        max_n = min(len(e.errors), 10)
        for i in range(1, max_n + 1):
            err = e.errors[i - 1]
            optDict = err['index']
            print(" " * 10, f"{i:3d} {optDict['status']}", optDict['_id'], optDict['error'])
        failed += len(e.errors)
    return success, failed
