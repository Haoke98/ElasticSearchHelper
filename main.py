# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2022/9/10
@Software: PyCharm
@disc:
======================================="""
import click
from dotenv import load_dotenv

import map
from task.task import show


@click.group()
def main():
    pass


@main.command()
@click.option("--obj2nested", flag_value=True, default=False)
@click.option("-i", "--input", help="input file,[Must be an CSV]")
@click.option("-o", "--output", default="index-map-generated.json", help="input file,[Must be an JSON file path.]")
def generate_map(input, output, obj2nested):
    map.generate(input, output, obj2nested)


@main.command()
@click.option("--id", help="Task ID")
def task(id):
    show(id)


if __name__ == '__main__':
    load_dotenv('/Users/shadikesadamu/SLRC/数据处理工具/python/.env')
    # show("wKzbDaI7SXqHimMRZBgHxg:84423362")
    # show("zJ9FtKuNSqi4mycedlvMRg:466125529")
    main()
    # export_field_table('hzxy_nation_global_enterprise')
