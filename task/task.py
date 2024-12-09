# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2022/9/10
@Software: PyCharm
@disc:
======================================="""
import datetime
import os
from elasticsearch import Elasticsearch


def show(id: str):
    """
    This is an method just for estimate the time of the task need.
    :param id:
    :return:
    """
    EsClient = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                             http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                             ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
    resp = EsClient.tasks.get(task_id=id)
    task = resp.get("task")
    status = task.get("status")
    completed = status.get("updated") + status.get("created") + status.get("deleted")
    total = status.get("total")
    progress = completed / total
    progress_in_percent = progress * 100
    if resp.get("completed"):
        print("状态：已结束")
        print(f"完成率：{progress_in_percent} %")
    else:
        print("状态：正在运行.....")
        left = total - completed
        running_time_in_nanos = task.get("running_time_in_nanos")
        running_time_in_millis = running_time_in_nanos * 10 ** -6
        running_time = datetime.timedelta(milliseconds=running_time_in_millis)
        speed_in_nanos = completed / running_time_in_nanos
        speed_in_millis = speed_in_nanos * 10 ** 6
        speed_in_second = speed_in_millis * 10 * 3
        speed_in_minute = speed_in_second * 60
        left_time_in_nanos = left / speed_in_nanos
        left_time_in_millis = left_time_in_nanos * 10 ** -6
        left_time = datetime.timedelta(milliseconds=left_time_in_millis)
        start_time_in_millis = task.get("start_time_in_millis")
        finishedAt = datetime.datetime.now() + left_time
        print(f"已运行：{running_time}")
        print(f"进度：{progress_in_percent} %")
        print("速率：")
        w = 6
        print("".rjust(w, " ") + f"{speed_in_nanos}/纳秒(nanosecond)")
        print("".rjust(w, " ") + f"{speed_in_millis}/毫秒(millisecond)")
        print("".rjust(w, " ") + f"{speed_in_second}/秒(second)")
        print("".rjust(w, " ") + f"{speed_in_minute}/分(minute)")
        print(f"预估需要：{left_time}")
        print(f"预估完成于：{finishedAt}")
