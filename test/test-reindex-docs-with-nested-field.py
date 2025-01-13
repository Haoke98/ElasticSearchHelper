import os
from elasticsearch import Elasticsearch
from tqdm import tqdm
from es_helper.map.reindex import load_field_mapping, transform_doc, bulk_operation

esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                      basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                      ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)


def test_nested_field(src_index: str, dst_index: str, field_name: str):
    resp = esCli.search(index=src_index, query={
        "nested": {
            "path": field_name,
            "query": {
                "match_all": {}
            }
        }
    }, size=1000)

    hits = resp["hits"]["hits"]
    total = len(hits)
    print(f"找到 {total} 条文档需要处理")

    # 使用tqdm创建进度条
    action_buffer = []  # 清空缓冲区
    for doc in tqdm(hits, desc="处理文档", unit="doc"):
        transformed_doc = transform_doc(doc, field_mapping, False)
        action_buffer.append({
            '_index': dst_index,
            '_id': doc['_id'],
            '_source': transformed_doc
        })
    bulk_operation(esCli, action_buffer)
    print("(Ok)")


if __name__ == '__main__':
    field_mapping = load_field_mapping(r"D:\Projects\IndexMap\reindex_v4_to_v5.csv")
    for nested_field_name in ["newsFeelingsData.dataList", "administrativePenaltyData.dataList",
                              "announcementData.dataList",]:
        test_nested_field("ent-mdsi-v4", "ent-mdsi-v6", nested_field_name)
