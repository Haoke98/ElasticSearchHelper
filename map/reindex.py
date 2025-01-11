import csv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
import os


def load_field_mapping(mapping_file):
    """
    加载字段映射关系
    """
    mapping = {}
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
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
                transformed[target_field] = value
            else:
                transformed[src_field] = value

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

    # 准备批量操作
    def process_docs():
        for doc in scan(esCli, index=source_index, scroll='5m', size=batch_size):
            transformed_doc = transform_doc(doc, field_mapping, strict_mode)
            yield {
                '_index': target_index,
                '_id': doc['_id'],
                '_source': transformed_doc
            }

    # 执行批量重建索引
    success, failed = 0, 0
    for ok, item in bulk(esCli, process_docs(), stats_only=False):
        if ok:
            success += 1
        else:
            failed += 1
            print(f"处理文档失败: {item}")

    print(f"重建索引完成: 成功 {success} 条, 失败 {failed} 条")
