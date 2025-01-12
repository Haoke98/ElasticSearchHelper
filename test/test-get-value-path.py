import json
import os

from elasticsearch import Elasticsearch

from es_helper.map.reindex import load_field_mapping, transform_doc, bulk_operation

if __name__ == '__main__':
    # 获取源字段的值
    strict_mode = False
    _id = '91410200769463127C'
    src_index = 'ent-mdsi-v4'
    dst_index = 'ent-mdsi-v5'
    src_field = 'jobInfoData.totalNum'
    esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                          http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                          ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
    doc = esCli.get(index=src_index, id=_id)
    print(json.dumps(doc.body, indent=4, ensure_ascii=False))

    # 加载字段映射
    field_mapping = load_field_mapping(r"D:\Projects\IndexMap\reindex_v4_to_v5.csv")
    transformed_doc = transform_doc(doc=doc, field_mapping=field_mapping, strict_mode=strict_mode)
    pass
    action_buffer = []  # 清空缓冲区
    action_buffer.append({
        '_index': dst_index,
        '_id': doc['_id'],
        '_source': transformed_doc
    })
    bulk_operation(esCli, action_buffer)
