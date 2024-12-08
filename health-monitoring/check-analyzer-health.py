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
from elasticsearch import Elasticsearch

load_dotenv('/Users/shadikesadamu/SLRC/数据处理工具/python/.env')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
es = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                   http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                   ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)


def check():
    global es
    resp = es.cat.plugins()
    print(resp)
    resp = es.cat.nodes(h="id,name,http_address")
    data_nodes = {}
    lines = resp.split("\n")
    for line in lines:
        line = line.strip()
        if line == '':
            break
        i = 0
        items = line.split(" ")
        item_list = []
        for item in items:
            item = item.strip()
            if item != '':
                item_list.append(item)
        data_nodes[item_list[0]] = {
            "name": item_list[1],
            "http_address": item_list[2],
        }
        pass
    for node_id, node in data_nodes.items():
        node_name = node["name"]
        http_address = node['http_address']
        es = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + http_address,
                           basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                           ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
        print(node_id, node_name, http_address, ":")
        analyzer_list = ["ik_max_word", "ik_smart"]
        for analyzer in analyzer_list:
            resp = es.indices.analyze(
                body={
                    "analyzer": analyzer,  # 指定IK分词器
                    "text": "这是要分析的文本"  # 待分析的文本
                }
            )
            print("\t", analyzer, "\t", resp)


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
