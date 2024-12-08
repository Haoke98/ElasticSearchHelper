# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/7
@Software: PyCharm
@disc:
======================================="""
import csv
import json

from map.generate_map import get_all_children


def core(_row, parent_field_full_name=None, parent_field_type=None, level: int = 1, nestedTypeMinLevel: int = 1):
    global i
    if len(_row) == 4:
        _field_name, _field_type, _ignore_above, _analyser = _row
    if len(_row) == 5:
        _field_name, _field_type, _ignore_above, _analyser, _field_full_name = _row
    if parent_field_full_name is None:
        _field_full_name = _field_name
    else:
        _field_full_name = str(parent_field_full_name) + "." + _field_name
    i += 1
    xxx = f"{_field_type}:{level}"
    print(str(i).rjust(3, " "), _field_full_name.ljust(80, " "), ":", "\t\t\t" * (level - 1), _field_name, xxx)
    field_data = {"type": _field_type}
    if _field_type == 'object':
        if parent_field_type != "text" and level <= nestedTypeMinLevel:
            # 如果上一个字段是text, 则这是一个multi-field字段, multi-field字段是不支持nested类型, 只支持keyword类型
            # Type [nested] cannot be used in multi field
            field_data["type"] = "nested"
            field_data["dynamic"] = True
        field_properties = {}
        children = get_all_children(_field_full_name, fields)
        for child in children:
            child_name = child[0]
            field_properties[child_name] = core(child, _field_full_name, _field_type, level + 1)
        if field_properties != {}:
            field_data['properties'] = field_properties
    elif _field_type == 'text':
        _fields = {}
        children = get_all_children(_field_full_name, fields)
        for child in children:
            child_name = child[0]
            _fields[child_name] = core(child, _field_full_name, _field_type, level + 1)
        if _fields != {}:
            field_data['fields'] = _fields
    else:
        pass
    if _ignore_above is not None and _ignore_above.strip() != "":
        field_data['ignore_above'] = int(_ignore_above)
    if _analyser is not None and _analyser.strip() != "":
        field_data['analyzer'] = _analyser
    return field_data


if __name__ == '__main__':
    i = 0
    fields = []
    with open('field-table.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            fields.append(row)

    properties = {}
    for row in fields:
        field_name, field_type, ignore_above, analyser = row
        if "." not in field_name:
            properties[field_name] = core(row)

    with open('mappings-generated-obj2nested.json', 'w', encoding='utf-8') as f_out:
        data = {"mappings": {"properties": properties}}
        f_out.write(json.dumps(data, indent=4, ensure_ascii=False))
