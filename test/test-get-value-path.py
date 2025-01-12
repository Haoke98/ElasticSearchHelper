import json
import os
import pprint

from elasticsearch import Elasticsearch

from map.reindex import get_value_by_path, set_value_by_path

if __name__ == '__main__':
    # 获取源字段的值
    strict_mode = False
    _id = '91410200769463127C'
    _index = 'ent-mdsi-v4'
    src_field = 'jobInfoData.totalNum'
    esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                          http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                          ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
    resp = esCli.get(index=_index, id=_id)
    print(json.dumps(resp.body, indent=4, ensure_ascii=False))
    source = resp["_source"]

    maps = {
        "jobInfoData.totalNum":"jobInfoDataTotalNum"
    }
    for src_field,target_field in maps.items():
        transformed = {} if strict_mode else source.copy()
        value = get_value_by_path(source, src_field)
        print(value)
        # 设置目标字段的值
        set_value_by_path(transformed, target_field, value)
        print(transformed)
        pass
