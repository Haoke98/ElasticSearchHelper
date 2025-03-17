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
import csv
from tqdm import tqdm

import click
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from es_helper.constants import APP_HOME_DIR, EXPORT_DIR
from es_helper.map import generate_full, generate_simplified, generate_meaning_guessed_field_table, \
    export_field_table as _export_field_table

from es_helper.map.reindex import custom_reindex
from es_helper.task import task
from es_helper import version
from es_helper.map.template import update_template_mapping
from es_helper.aggs.export import export_aggs_to_csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(APP_HOME_DIR):
    os.mkdir(APP_HOME_DIR)
if not os.path.exists(EXPORT_DIR):
    os.mkdir(EXPORT_DIR)


def load_env_file(ctx, param, value):
    """加载环境变量文件"""
    if value:
        load_dotenv(value)
    else:
        load_dotenv()  # 默认加载当前目录的 .env 文件


@click.group()
@click.option('-e', '--env-file',
              type=click.Path(exists=True, file_okay=True, dir_okay=False),
              help='指定环境变量文件路径',
              callback=load_env_file)
def main(env_file):
    """ES-Helper 命令行工具"""
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
@click.option("--skip", default=0, help="跳过前n个文档", type=int)
def reindex(source, destination, mapping, batch_size, strict, skip):
    """
    根据映射关系重建索引
    """
    custom_reindex(source, destination, mapping, batch_size, strict, skip)


@main.command()
def version():
    """显示版本信息"""
    click.echo(f"ES-Helper v{version.__version__}")
    click.echo(f"author: {version.__author__}")
    click.echo(f"author email: {version.__email__}")
    click.echo(f"project url: {version.__url__}")


@main.command()
@click.option("-i", "--input", help="input file,[Must be an CSV]", required=True)
@click.option("-t", "--template", help="Template name to update", required=True)
@click.option("--obj2nested", flag_value=True, default=False)
@click.option("-f", "--full", help="Generate on full mode.", flag_value=True, default=False)
def update_template(input, template, full, obj2nested):
    """
    生成mapping并更新到指定的索引模板
    """
    # 生成临时mapping文件
    temp_mapping_file = os.path.join(EXPORT_DIR,
                                     f"temp-mapping-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")

    # 生成mapping
    if full:
        generate_full(input, temp_mapping_file, obj2nested)
    else:
        generate_simplified(input, temp_mapping_file, obj2nested)

    try:
        # 创建ES客户端
        es_client = Elasticsearch(
            hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
            basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
            ca_certs=os.getenv("SLRC_ES_CA"),
            request_timeout=3600
        )

        # 更新模板
        update_template_mapping(es_client, template, temp_mapping_file)

    finally:
        # 清理临时文件
        if os.path.exists(temp_mapping_file):
            os.remove(temp_mapping_file)


@main.command()
@click.option("-i", "--index", help="索引名称", prompt="请输入索引名称")
@click.option("-f", "--field", help="聚合字段", prompt="请输入要聚合的字段名")
@click.option("-s", "--size", help="聚合桶的大小(最大655360)", default=10000, type=click.IntRange(1, 655360),
              prompt="请输入聚合桶的大小(默认10000，最大655360)",
              prompt_required=False)
def export_aggs(index, field, size):
    """
    导出字段聚合结果到CSV文件
    """
    try:
        # 创建ES客户端
        es_client = Elasticsearch(
            hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
            basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
            ca_certs=os.getenv("SLRC_ES_CA"),
            request_timeout=7200  # 增加客户端默认超时时间到2小时
        )

        # 执行导出
        click.echo("开始执行聚合查询...")
        csv_path, aggs_result = export_aggs_to_csv(es_client, index, field, size)

        # 打印结果
        click.echo(f"\n聚合结果已导出到: {csv_path}")
        click.echo(f"总计 {len(aggs_result['buckets'])} 个唯一值")
        click.echo(f"文档总数: {sum(bucket['doc_count'] for bucket in aggs_result['buckets'])}")

    except ValueError as e:
        click.echo(f"错误: {str(e)}", err=True)
    # except Exception as e:
    #     click.echo(f"发生错误: {str(e)}", err=True)
    #     click.echo("可能的解决方案：", err=True)
    #     click.echo(f"1. 减小聚合桶的大小（当前：{size}）", err=True)
    #     click.echo("2. 确保字段名称正确且已建立索引", err=True)
    #     click.echo("3. 检查集群状态和资源使用情况", err=True)


@main.command()
@click.option("-i", "--input", help="字段映射表CSV文件路径", required=True)
@click.option("-idx", "--index", help="要分析的ES索引名称", required=True)
@click.option("-o", "--output", help="输出CSV文件路径")
@click.option("-b", "--batch-size", default=1000, help="每批处理的字段数量", type=int)
def analyze_field_coverage(input, index, output, batch_size):
    """
    分析索引中字段的覆盖率

    输入CSV格式：字段名|数据类型|字段说明|状态
    输出CSV格式：字段名|数据类型|字段说明|状态|count|proportion
    """
    # 如果未指定输出文件，则基于输入文件生成默认输出文件名
    if output is None:
        output = os.path.join(EXPORT_DIR,
                              f"{os.path.splitext(os.path.basename(input))[0]}-coverage-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.csv")

    # 创建ES客户端
    es_client = Elasticsearch(
        hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
        basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
        ca_certs=os.getenv("SLRC_ES_CA"),
        request_timeout=3600
    )

    # 获取索引总文档数
    total_docs = es_client.count(index=index)["count"]
    click.echo(f"索引 {index} 共有 {total_docs} 个文档")

    # 读取字段映射CSV文件
    fields = []
    with open(input, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='|')
        headers = next(reader)  # 读取表头
        for row in reader:
            fields.append(row)

    # 准备新的CSV表头（添加count和proportion列）
    new_headers = headers + ["count", "proportion"]

    # 分析每个字段的覆盖率
    results = []
    click.echo(f"正在分析 {len(fields)} 个字段的覆盖率...")

    # 使用tqdm显示进度
    for field_data in tqdm(fields, desc="分析字段覆盖率"):
        field_name = field_data[0]
        field_type = field_data[1]

        # 根据字段类型构建查询
        if field_type == "nested":
            # 处理nested类型字段
            path_parts = field_name.split('.')
            if len(path_parts) > 1:
                # 获取nested路径（去掉最后一个部分）
                nested_path = '.'.join(path_parts[:-1])
                query = {
                    "nested": {
                        "path": nested_path,
                        "query": {
                            "exists": {
                                "field": field_name
                            }
                        }
                    }
                }
            else:
                # 如果字段名不包含点，则使用普通exists查询
                query = {"exists": {"field": field_name}}
        else:
            # 非nested类型字段使用标准exists查询
            query = {"exists": {"field": field_name}}

        # 执行查询
        try:
            field_count = es_client.count(index=index, query=query)["count"]
        except Exception as e:
            click.echo(f"查询字段 {field_name} 时出错: {str(e)}", err=True)
            field_count = 0

        # 计算覆盖率
        coverage = field_count / total_docs if total_docs > 0 else 0

        # 添加count和proportion到字段数据
        row_with_coverage = field_data + [
            str(field_count),
            f"{coverage:.4f}"  # 四位小数的覆盖率
        ]
        results.append(row_with_coverage)

    # 写入结果到CSV文件
    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    with open(output, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(new_headers)
        writer.writerows(results)

    click.echo(f"\n字段覆盖率分析完成，结果已保存到: {output}")


if __name__ == '__main__':
    main()
