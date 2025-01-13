import csv
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan, BulkIndexError

error_reason_map = {}


def load_field_mapping(mapping_file):
    """
    加载字段映射关系
    """
    mapping = {}
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            source_field = row['source_field']
            target_field = row['target_field']
            mapping[source_field] = target_field
    return mapping


def get_value_by_path(obj, path):
    """
    根据路径获取JSON对象中的值
    
    Args:
        obj: JSON对象
        path: 字段路径，如 "jobInfoData.TotalNum"
    """
    current = obj
    for part in path.split('.'):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def set_value_by_path(obj, path, value):
    """
    根据路径设置JSON对象中的值
    
    Args:
        obj: JSON对象
        path: 字段路径，如 "jobInfoDataTotalNum" 或 "jobInfoData"
        value: 要设置的值
    """
    parts = path.split('.')
    current = obj
    
    # 如果是单层路径，直接设置
    if len(parts) == 1:
        obj[path] = value
        return
        
    # 遍历路径的每一部分，除了最后一个
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    
    # 设置最后一个字段的值
    current[parts[-1]] = value


def delete_value_by_path(obj, path):
    """
    根据路径删除JSON对象中的值
    
    Args:
        obj: JSON对象
        path: 字段路径，如 "jobInfoData.TotalNum"
    """
    parts = path.split('.')
    current = obj
    
    # 遍历到倒数第二层
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return
    
    # 删除最后一个字段
    if isinstance(current, dict) and parts[-1] in current:
        del current[parts[-1]]
        
    # 如果删除后父对象为空，也删除父对象
    if len(parts) > 1 and not current:
        delete_value_by_path(obj, '.'.join(parts[:-1]))


def get_parent_field(field_path):
    """
    获取字段的父级字段
    
    Args:
        field_path: 字段路径，如 "jobInfoData.dataList.entName"
    Returns:
        父级字段路径，如 "jobInfoData.dataList"
    """
    parts = field_path.split('.')
    return '.'.join(parts[:-1])


def get_field_name(field_path):
    """
    获取字段名称
    
    Args:
        field_path: 字段路径，如 "jobInfoData.dataList.entName"
    Returns:
        字段名称，如 "entName"
    """
    return field_path.split('.')[-1]


def transform_doc(doc, field_mapping, strict_mode=False):
    """
    根据映射关系转换文档
    
    Args:
        doc: 源文档
        field_mapping: 字段映射关系
        strict_mode: 是否启用严格模式
    """
    # 克隆源文档
    source = doc['_source'].copy()
    transformed = {} if strict_mode else source.copy()
    
    # 收集数组映射信息
    array_mappings = {}  # 存储数组相关的映射信息
    field_excludes = {}  # 存储每个数组需要排除的字段
    
    # 首先收集所有映射信息
    for src_field, target_field in field_mapping.items():
        parts = src_field.split('.')
        if len(parts) > 2 and parts[-2] == 'dataList':
            # 处理数组元素的字段映射
            array_path = '.'.join(parts[:-1])  # 如: jobInfoData.dataList
            field_name = parts[-1]  # 如: entName
            
            if array_path not in field_excludes:
                field_excludes[array_path] = set()
            
            if not target_field:  # 如果目标字段为空，加入排除列表
                field_excludes[array_path].add(field_name)
        elif parts[-1] == 'dataList':
            # 记录数组本身的映射
            array_mappings[src_field] = target_field
    
    # 按照映射表重构文档
    for src_field, target_field in field_mapping.items():
        if not target_field:  # 如果目标字段为空，删除该字段
            delete_value_by_path(transformed, src_field)
            continue
            
        # 获取源字段的值
        value = get_value_by_path(source, src_field)
        if value is not None:
            if src_field in array_mappings:
                # 处理数组字段
                if isinstance(value, list):
                    # 过滤数组中每个对象的字段
                    filtered_value = []
                    exclude_fields = field_excludes.get(src_field, set())
                    for item in value:
                        if isinstance(item, dict):
                            filtered_item = {k: v for k, v in item.items() 
                                          if k not in exclude_fields}
                            if filtered_item:  # 只有当过滤后的对象非空时才添加
                                filtered_value.append(filtered_item)
                    value = filtered_value
            
            # 设置目标字段的值
            set_value_by_path(transformed, target_field, value)
            # 删除原始字段
            delete_value_by_path(transformed, src_field)
    
    # 对transformed的key进行排序
    transformed = dict(sorted(transformed.items()))
    return transformed


def custom_reindex(source_index, target_index, mapping_file, batch_size=1000, strict_mode=False, skip=0):
    """
    执行自定义reindex操作
    
    Args:
        source_index: 源索引名称
        target_index: 目标索引名称
        mapping_file: 字段映射文件路径
        batch_size: 批处理大小
        strict_mode: 是否启用严格模式
        skip: 跳过前n个文档
    """
    # 获取ES连接
    esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                          http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                          ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)

    # 加载字段映射
    field_mapping = load_field_mapping(mapping_file)

    # 修改处理文档的方式
    action_buffer = []
    total_processed = 0  # 从0开始计数总处理数
    success, failed = skip, 0  # 成功数从skip开始计数

    try:
        # 获取总文档数
        total_docs = esCli.count(index=source_index)['count']
        
        if skip > 0:
            print(f"正在跳过前 {skip} 个文档...")

        # 使用scan遍历文档
        for doc in scan(esCli, index=source_index, scroll='5m', size=batch_size):
            total_processed += 1
            
            if total_processed <= skip:  # 跳过前skip个文档
                if total_processed % 10000 == 0:  # 每跳过10000个文档显示一次进度
                    print(f"\r跳过进度：{total_processed}/{skip} "
                          f"({(total_processed / skip * 100):.2f}%)", end='')
                continue
            
            if total_processed == skip + 1:
                print("\n开始处理文档...")
                
            transformed_doc = transform_doc(doc, field_mapping, strict_mode)
            action_buffer.append({
                '_index': target_index,
                '_id': doc['_id'],
                '_source': transformed_doc
            })

            # 当缓冲区达到batch_size时执行批量操作
            if len(action_buffer) >= batch_size:
                # 执行批量操作
                success_batch, failed_batch = bulk_operation(esCli, action_buffer)
                success += success_batch
                failed += failed_batch

                print(f"\r进度：{total_processed}/{total_docs} "
                      f"({(total_processed / total_docs * 100):.2f}%) "
                      f"成功：{success}, 失败：{failed}", end='')

                action_buffer = []  # 清空缓冲区

        # 处理剩余的文档
        if action_buffer:
            success_batch, failed_batch = bulk_operation(esCli, action_buffer)
            success += success_batch
            failed += failed_batch
            total_processed += len(action_buffer)

        print(f"\n重建索引完成: 成功 {success} 条, 失败 {failed} 条")

    except Exception as e:
        logging.error(f"Exception!:{e}", exc_info=True)


def bulk_operation(client, actions):
    """执行批量操作并返回成功失败数"""
    success = failed = 0
    try:
        _success, errors = bulk(client, actions, stats_only=False)
        success += _success
        failed += len(errors)
    except BulkIndexError as e:
        for i, err in enumerate(e.errors, 1):
            opt_dict = err['index']
            status = opt_dict['status']
            err_dict = opt_dict['error']
            err_type = err_dict['type']
            err_reason = err_dict['reason']
            if err_type == 'strict_dynamic_mapping_exception':
                if not error_reason_map.__contains__(err_reason):
                    print("\r", f"{i:3d} {status}", opt_dict['_id'], err_dict)
                    print()
                    error_reason_map[err_reason] = err
                pass
            else:
                print(" " * 10, f"{i:3d} {status}", opt_dict['_id'], err_dict)
        failed += len(e.errors)
    return success, failed
