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

from es_helper.constants import APP_HOME_DIR, EXPORT_DIR
from es_helper.map import generate_full, generate_simplified, generate_meaning_guessed_field_table, \
    export_field_table as _export_field_table

from es_helper.map.reindex import custom_reindex
from es_helper.task import task
from es_helper import version

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(APP_HOME_DIR):
    os.mkdir(APP_HOME_DIR)
if not os.path.exists(EXPORT_DIR):
    os.mkdir(EXPORT_DIR)


@click.group()
def main():
    pass


@main.command()
@click.option("-i", "--input", help="input file,[Must be an CSV]", required=True)
@click.option("-o", "--output", help="input file,[Must be an JSON file path.]")
@click.option("--obj2nested", flag_value=True, default=False)
@click.option("-f", "--full", help="Generate on full mode.", flag_value=True, default=False)
def generate_map(input, output, full, obj2nested):
    """
    生成索引mappings
    """
    if output is None:
        output = os.path.join(EXPORT_DIR,
                              f"index-map-generated-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")
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
    task.show(id)


@main.command()
@click.option("-s", "--source", help="源索引名称", required=True)
@click.option("-d", "--destination", help="目标索引名称", required=True)
@click.option("-m", "--mapping", help="字段映射文件路径 (CSV格式)", required=True)
@click.option("-b", "--batch-size", default=1000, help="每批处理的文档数量", type=int)
@click.option("--strict", is_flag=True, default=False, help="启用严格模式（只映射在映射表中定义的字段）")
def reindex(source, destination, mapping, batch_size, strict):
    """
    根据映射关系重建索引
    """
    custom_reindex(source, destination, mapping, batch_size, strict)


@main.command()
def version():
    """显示版本信息"""
    click.echo(f"ES-Helper v{version.__version__}")
    click.echo(f"author: {version.__author__}")
    click.echo(f"author email: {version.__email__}")
    click.echo(f"project url: {version.__url__}")


if __name__ == '__main__':
    main()
