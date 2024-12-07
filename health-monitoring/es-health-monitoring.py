# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/3/21
@Software: PyCharm
@disc:
======================================="""
import csv
import datetime
import json
import os.path
import time
from lib import esHelper

esClient = esHelper.get_con()


def health_check(n: int = 1):
    curr_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    dirname = os.path.join("cluster-states", curr_time)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    infoResp = esClient.info()
    with open(f"{dirname}/info.json", 'w') as f:
        json.dump(dict(infoResp), f, indent=4)
    healthResp = esClient.cluster.health()
    with open(f"{dirname}/health.json", 'w') as f:
        json.dump(dict(healthResp), f, indent=4)
    print(esClient.cat.nodes(h="id,name,master,role,http,version"))
    # print(esClient.cat.indices())
    healthCSV = os.path.join("cluster-states", "health.csv")
    header_row = ['time', 'cluster_id']
    for key in healthResp.raw.keys():
        header_row.append(key)
    if n == 1:
        print(n, header_row)
    if os.path.exists(healthCSV):
        csvf = open(healthCSV, 'a+')
        writer = csv.writer(csvf)
    else:
        csvf = open(healthCSV, 'w')
        writer = csv.writer(csvf)
        writer.writerow(header_row)
    row = [curr_time, infoResp.raw.get("cluster_uuid")]
    for key in healthResp.raw.keys():
        row.append(healthResp.raw.get(key))
    print(n, row)
    writer.writerow(row)
    csvf.close()


if __name__ == '__main__':
    n = 1
    while True:
        health_check(n)
        time.sleep(5)
        n += 1
    # print(esClient.cat.shards())
    # print(esClient.cat.indices(index="market-subjects"))
    # print(esClient.count(index="market-subjects-merged-v3"))
    # print(esClient.cat.allocation(v=True))
    # print(esClient.snapshot.get_repository())
    # print(esClient.indices.recovery(index="market-subjects-merged-v3"))
    # print(esClient.cluster.health())
    # print(esClient.snapshot.create_repository(name="market_subjects", type="fs",
    #                                           settings={"location": "/home/es-repo/backups/market_subjects"}))
    # print(esClient.snapshot.get(repository="market_subjects"))
    # print(esClient.cluster.allocation_explain())
    # print(esClient.cluster.state())
