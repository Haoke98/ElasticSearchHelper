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


def transform_doc(doc, field_mapping):
    """
    根据映射关系转换文档
    """
    transformed = {}
    source = doc['_source']

    for src_field, value in source.items():
        if src_field in field_mapping:
            target_field = field_mapping[src_field]
            if target_field:  # 只有当目标字段不为空时才进行映射
                transformed[target_field] = value

    return transformed


def custom_reindex(source_index, target_index, mapping_file, batch_size=1000):
    """
    执行自定义reindex操作
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
            transformed_doc = transform_doc(doc, field_mapping)
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
