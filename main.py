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

import click
from dotenv import load_dotenv

from map import generate_meaning_guessed_field_table, generate_full, generate_simplified, \
    export_field_table as _export_field_table
from task.task import show
from map.reindex import custom_reindex

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(BASE_DIR, 'export')


@click.group()
def main():
    pass


@main.command()
@click.option("-i", "--input", help="input file,[Must be an CSV]", required=True)
@click.option("-o", "--output", default=f"index-map-generated-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json",
              help="input file,[Must be an JSON file path.]")
@click.option("--obj2nested", flag_value=True, default=False)
@click.option("-f", "--full", help="Generate on full mode.", flag_value=True, default=False)
def generate_map(input, output, full, obj2nested):
    """
    生成索引mappings
    """
    if full:
        generate_full(input, output, obj2nested)
    else:
        generate_simplified(input, output, obj2nested)


@main.command()
@click.option("-i", "--index", help="input the index name for export")
@click.option("-d", "--export_dir", default=EXPORT_DIR, help="export dir")
@click.option("-gsm", "--guess-meaning", help="要不要通过LLM猜测出字段实际意义", flag_value=True, default=False)
def export_field_table(index, export_dir, guess_meaning):
    """
    从ES中导出现有索引的字段列表
    """
    if not os.path.exists(export_dir):
        os.mkdir(export_dir)

    csv_out_fp = _export_field_table(index, export_dir)
    if guess_meaning:
        meaning_guessed_out_fp = os.path.join(export_dir, f"{csv_out_fp[:-4]}-guessed.csv")
        generate_meaning_guessed_field_table(csv_out_fp, meaning_guessed_out_fp)


@main.command()
@click.option("--id", help="Task ID", required=True)
def task(id):
    show(id)


@main.command()
@click.option("-s", "--source", help="源索引名称", required=True)
@click.option("-d", "--destination", help="目标索引名称", required=True)
@click.option("-m", "--mapping", help="字段映射文件路径 (CSV格式)", required=True)
@click.option("-b", "--batch-size", default=1000, help="每批处理的文档数量", type=int)
def reindex(source, destination, mapping, batch_size):
    """
    根据映射关系重建索引
    """
    custom_reindex(source, destination, mapping, batch_size)


if __name__ == '__main__':
    load_dotenv('/Users/shadikesadamu/.config/.env')
    # show("wKzbDaI7SXqHimMRZBgHxg:84423362")
    # show("zJ9FtKuNSqi4mycedlvMRg:466125529")
    main()
    # export_field_table('hzxy_nation_global_enterprise')
