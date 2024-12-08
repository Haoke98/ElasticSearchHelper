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


def get_all_children(parent_field_full_name: str, _fields):
    """

    :param parent_field_full_name: full name
    :return:
    """
    parent_field_parts = parent_field_full_name.split('.')
    result = []
    for _row in _fields:
        (child_field_full_name, child_field_type, child_field_ignore_above, child_field_analyser) = _row
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
                        result.append([child_field_name, child_field_type, child_field_ignore_above,
                                       child_field_analyser, child_field_full_name])
    return result


def core(_row, parent_field_full_name=None, level: int = 1):
    global i, fields
    if len(_row) == 4:
        _field_name, _field_type, _ignore_above, _analyser = _row
    if len(_row) == 5:
        _field_name, _field_type, _ignore_above, _analyser, _field_full_name = _row
    if parent_field_full_name is None:
        _field_full_name = _field_name
    else:
        _field_full_name = str(parent_field_full_name) + "." + _field_name
    i += 1
    print(str(i).rjust(3, " "), _field_full_name.ljust(80, " "), ":", "\t\t\t" * (level - 1), _field_name)
    field_data = {"type": _field_type}
    if _field_type == 'object':
        field_properties = {}
        children = get_all_children(_field_full_name, fields)
        for child in children:
            child_name = child[0]
            field_properties[child_name] = core(child, _field_full_name, level + 1)
        if field_properties != {}:
            field_data['properties'] = field_properties
    elif _field_type == 'text':
        _fields = {}
        children = get_all_children(_field_full_name, fields)
        for child in children:
            child_name = child[0]
            _fields[child_name] = core(child, _field_full_name, level + 1)
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

    with open('mappings_generated.json', 'w', encoding='utf-8') as f_out:
        data = {"mappings": {"properties": properties}}
        f_out.write(json.dumps(data, indent=4, ensure_ascii=False))
