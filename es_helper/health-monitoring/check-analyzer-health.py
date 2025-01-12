# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/8
@Software: PyCharm
@disc:
======================================="""
import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, SSLError, RequestError

load_dotenv('/Users/shadikesadamu/.config/.env')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
es = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                   http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                   ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)


def check():
    global es
    resp = es.cat.plugins()
    print(resp)
    nodes_info = es.nodes.info()
    for node_id, node_info in nodes_info['nodes'].items():
        node_name = node_info["name"]
        http_address = node_info['http']['publish_address']
        roles = node_info['roles']
        print("NODE_ID:", node_id, http_address, node_name, roles, ':')
        es = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + http_address,
                           http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                           ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
        analyzer_list = ["ik_max_word", "ik_smart"]
        for analyzer in analyzer_list:
            print("\t\t", str(analyzer).ljust(16, " "), end=':\t    ')
            try:
                resp = es.indices.analyze(
                    body={
                        "analyzer": analyzer,  # 指定IK分词器
                        "text": "这是要分析的文本"  # 待分析的文本
                    }
                )
                print("Ok!")
            except SSLError as e:
                print("证书异常")
            except RequestError as e:
                if e.status_code == 400:
                    _error = e.info.get('error')
                    root_case = _error.get('root_case')
                    reason = _error.get('reason')
                    if reason.startswith("failed to find global analyzer"):
                        print("该节点没有分词器")
                        continue
                raise e


def check_on_update():
    index = "hzxy_nation_global_enterprise"
    resp = es.update(index=index, id="7880c0a2fb534fa7aa8a7c84bc739475", doc={
        "specialTagArr": [],
        "specialTagStr": "",
        "affirmChainCateStr": "煤炭煤电煤化工;优质畜(禽)产品;新能源;通用航空",
        "affirmChainStr": "现代煤化工;现代石化;乳制品;新能源和电力;农机装备;通用航空",
        "affirmChainNodeStr": "塑料行业;塑料;橡胶"
    })
    print(resp)


if __name__ == '__main__':
    check()
    # check_on_update()
