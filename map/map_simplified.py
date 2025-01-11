# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/12
@Software: PyCharm
@disc:
======================================="""
import copy
import csv
import json

from map.constants import TYPE_NESTED_MAX_LIMIT, DEFAULT_MAP

# 全局变量
type_nested_count = 0
i = 0


def get_all_children(parent_field_full_name: str, _fields):
    """

    :param parent_field_full_name: full name
    :return:
    """
    parent_field_parts = parent_field_full_name.split('.')
    result = []
    for _row in _fields:
        (child_field_full_name, child_field_type) = _row[0:2]
        if child_field_full_name.startswith(parent_field_full_name):
            if child_field_full_name != parent_field_full_name:
                child_field_parts = child_field_full_name.split('.')
                if len(parent_field_parts) + 1 == len(child_field_parts):
                    matched = True
                    for i in range(len(parent_field_parts)):
                        if parent_field_parts[i] != child_field_parts[i]:
                            matched = False
                    if matched:
                        child_field_name = child_field_parts[- 1]
                        result.append([child_field_name, child_field_type, child_field_full_name])
    return result


def generate_simplified(input_csv_fp: str, output_json_fp: str, obj2nested: bool = False):
    fields = []
    with open(input_csv_fp, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            fields.append(row)

    def core(_row, parent_field_full_name=None, parent_field_type=None, level: int = 1, nestedTypeMinLevel: int = 1):
        global type_nested_count, i
        _field_name, _field_type = _row[0:2]
        if parent_field_full_name is None:
            _field_full_name = _field_name
        else:
            _field_full_name = str(parent_field_full_name) + "." + _field_name
        i += 1
        xxx = f"{_field_type}:{level}"
        print(str(i).rjust(3, " "), _field_full_name.ljust(80, " "), ":", "\t\t\t" * (level - 1), _field_name, xxx)
        field_data = {"type": _field_type}
        if (
                _field_type == 'object' and level <= nestedTypeMinLevel and type_nested_count < TYPE_NESTED_MAX_LIMIT and obj2nested) or _field_type == "nested":
            field_data["type"] = "nested"
            field_data["dynamic"] = False
            type_nested_count += 1

        if _field_type in ['nested', 'object']:
            field_properties = {}
            children = get_all_children(_field_full_name, fields)
            for child in children:
                child_name = child[0]
                field_properties[child_name] = core(child, _field_full_name, _field_type, level + 1)
            if field_properties != {}:
                field_data['properties'] = field_properties

        elif _field_type == 'text':
            field_data["analyzer"] = "ik_smart"
            _fields = {"keyword": {"type": "keyword", "ignore_above": 256}}  # 默认keyword,如果CSV表格中已经有定义则会被覆盖
            children = get_all_children(_field_full_name, fields)
            for child in children:
                child_name = child[0]
                _fields[child_name] = core(child, _field_full_name, _field_type, level + 1)
            if _fields != {}:
                field_data['fields'] = _fields
        else:
            pass
        return field_data

    properties = {}
    for row in fields:
        field_full_name = row[0]
        if "." not in field_full_name:
            properties[field_full_name] = core(row)

    with open(output_json_fp, 'w', encoding='utf-8') as f_out:
        data = copy.copy(DEFAULT_MAP)
        data['properties'] = properties
        f_out.write(json.dumps(data, indent=4, ensure_ascii=False))
