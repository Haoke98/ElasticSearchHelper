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
@click.option("-d", "--delimiter", default=",", help="CSV文件分隔符", type=str)
@click.option("--field-col", default=0, help="字段名称列索引", type=int)
@click.option("--type-col", default=1, help="字段类型列索引", type=int)
@click.option("--count-col", default=4, help="Count列索引", type=int)
@click.option("--proportion-col", default=5, help="Proportion列索引", type=int)
@click.option("--update-existing", is_flag=True, default=True, help="更新现有的Count和Proportion列而不是添加新列")
@click.option("--safe-nested", is_flag=True, default=True, help="安全模式处理嵌套字段查询(避免嵌套路径不存在错误)")
def analyze_field_coverage(input, index, output, batch_size, delimiter, field_col, type_col, count_col, proportion_col, update_existing, safe_nested):
    """
    分析索引中字段的覆盖率
    
    默认输入CSV格式：字段名|数据类型|字段说明|状态|Count|Proportion
    输出CSV格式：更新现有Count和Proportion列的值
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
    
    # 获取索引映射，找出真正的嵌套字段
    actual_nested_fields = set()
    if safe_nested:
        try:
            mapping = es_client.indices.get_mapping(index=index)
            # 处理映射，提取嵌套字段路径
            index_mappings = list(mapping.values())[0]['mappings']
            
            def extract_nested_paths(properties, parent_path=""):
                for field_name, field_info in properties.items():
                    current_path = f"{parent_path}.{field_name}" if parent_path else field_name
                    if field_info.get('type') == 'nested':
                        actual_nested_fields.add(current_path)
                    if 'properties' in field_info:
                        extract_nested_paths(field_info['properties'], current_path)
            
            if 'properties' in index_mappings:
                extract_nested_paths(index_mappings['properties'])
            
            click.echo(f"从索引中检测到的嵌套字段: {sorted(actual_nested_fields)}")
        except Exception as e:
            click.echo(f"获取索引映射时出错: {str(e)}")
            click.echo("将使用CSV文件中的类型信息进行查询构建")

    # 读取字段映射CSV文件
    fields = []
    with open(input, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)  # 读取表头
        for row in reader:
            if row:  # 确保行不为空
                fields.append(row)
    
    # 打印CSV信息供调试
    click.echo(f"CSV表头: {headers}")
    click.echo(f"读取到 {len(fields)} 行数据")
    if fields:
        click.echo(f"第一行数据示例: {fields[0]}")
    
    # 准备输出的结果
    results = []
    click.echo(f"正在分析 {len(fields)} 个字段的覆盖率...")
    
    # 使用tqdm显示进度
    for field_data in tqdm(fields, desc="分析字段覆盖率"):
        try:
            # 安全获取字段名和类型，处理索引超出范围的情况
            field_name = field_data[field_col] if field_col < len(field_data) else ''
            field_type = field_data[type_col] if type_col < len(field_data) else 'unknown'
            
            if not field_name:  # 如果字段名为空，跳过
                click.echo(f"警告: 跳过空字段名的行: {field_data}")
                continue
            
            # 判断嵌套查询策略
            use_nested_query = False
            nested_path = None
            
            # 处理嵌套字段和嵌套字段的子字段
            if "." in field_name:
                path_parts = field_name.split('.')
                for i in range(len(path_parts)):
                    # 尝试各种可能的父路径
                    potential_path = '.'.join(path_parts[:i+1])
                    if potential_path in actual_nested_fields:
                        nested_path = potential_path
                        use_nested_query = True
                        break
            elif field_type == 'nested' and field_name in actual_nested_fields:
                nested_path = field_name
                use_nested_query = True
            
            # 构建查询
            if use_nested_query and nested_path:
                # 使用嵌套查询
                if field_name == nested_path:
                    # 字段本身是嵌套字段
                    query = {
                        "nested": {
                            "path": nested_path,
                            "query": {
                                "match_all": {}
                            }
                        }
                    }
                else:
                    # 嵌套字段的子字段
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
                # 使用普通exists查询
                query = {"exists": {"field": field_name}}
            
            # 执行查询
            try:
                field_count = es_client.count(index=index, query=query)["count"]
                # 只为嵌套字段输出调试信息
                if field_type == "nested" or "." in field_name:
                    if use_nested_query:
                        click.echo(f"字段 {field_name} 使用嵌套查询(路径:{nested_path}): 结果: {field_count}")
                    else:
                        click.echo(f"字段 {field_name} 使用普通查询: 结果: {field_count}")
            except Exception as e:
                click.echo(f"查询字段 {field_name} 时出错: {str(e)}", err=True)
                # 尝试使用普通exists查询作为后备
                try:
                    fallback_query = {"exists": {"field": field_name}}
                    field_count = es_client.count(index=index, query=fallback_query)["count"]
                    click.echo(f"字段 {field_name} 回退到普通查询: 结果: {field_count}")
                except Exception as fallback_error:
                    click.echo(f"回退查询也失败: {str(fallback_error)}", err=True)
                    field_count = 0
            
            # 计算覆盖率
            coverage = field_count / total_docs if total_docs > 0 else 0
            
            # 创建新行，更新或添加count和proportion值
            new_row = list(field_data)  # 复制原始行
            
            if update_existing:
                # 确保行长度足够存放count和proportion
                while len(new_row) <= max(count_col, proportion_col):
                    new_row.append('')
                
                # 更新现有列
                new_row[count_col] = str(field_count)
                new_row[proportion_col] = f"{coverage:.4f}"
            else:
                # 添加新列
                new_row += [str(field_count), f"{coverage:.4f}"]
            
            results.append(new_row)
            
        except Exception as e:
            click.echo(f"处理行 {field_data} 时出错: {str(e)}", err=True)
            continue
    
    # 准备输出的表头
    if update_existing:
        new_headers = list(headers)  # 使用原有表头
    else:
        new_headers = list(headers) + ["count", "proportion"]  # 添加新列
    
    # 写入结果到CSV文件
    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    with open(output, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerow(new_headers)
        writer.writerows(results)
    
    click.echo(f"\n字段覆盖率分析完成，结果已保存到: {output}")


if __name__ == '__main__':
    main()
