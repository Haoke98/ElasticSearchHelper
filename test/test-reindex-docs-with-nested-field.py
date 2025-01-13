import os

from elasticsearch import Elasticsearch

from es_helper.map.reindex import load_field_mapping, transform_doc, bulk_operation

esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                      basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                      ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)


def test_nested_field(field_name: str):
    resp = esCli.search(query={
        "nested": {
            "path": field_name,
            "query": {
                "match_all": {}
            }
        }
    })
    pass
    hits = resp["hits"]["hits"]
    for hit in hits:
        transform_doc(hit, field_mapping, False)
        pass


if __name__ == '__main__':
    field_mapping = load_field_mapping(r"D:\Projects\IndexMap\reindex_v4_to_v5.csv")
    test_nested_field("newsFeelingsData")
