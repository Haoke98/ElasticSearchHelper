# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/6
@Software: PyCharm
@disc:
======================================="""
import csv
import json


#
def parse_field_content(properties: dict, parent_field_name=None):
    for field_name, field_map in properties.items():
        if parent_field_name is not None:
            final_field_name = f"{parent_field_name}.{field_name}"
        else:
            final_field_name = field_name
        field_type = None
        ignore_above = None
        analyser = None
        field_map = dict(field_map)
        if dict(field_map).__contains__("type"):
            field_type = dict(field_map).get("type")
        elif dict(field_map).__contains__("properties"):
            field_type = "object"
        else:
            pass

        if dict(field_map).__contains__("ignore_above"):
            ignore_above = field_map.get("ignore_above")
        if dict(field_map).__contains__("analyzer"):
            analyser = field_map.get("analyzer")

        print(final_field_name.ljust(60, " "), field_type.ljust(20, " "), str(ignore_above).ljust(20, " "),
              str(analyser).ljust(20, " "))
        writer.writerow([final_field_name, field_type, ignore_above, analyser])
        if field_type == "object" and dict(field_map).__contains__("properties"):
            parse_field_content(field_map["properties"], final_field_name)
        if dict(field_map).__contains__("fields"):
            parse_field_content(field_map["fields"], final_field_name)


def main(_jsonfile):
    data: dict = json.load(_jsonfile)
    for index, v in data.items():
        mappings = v['mappings']
        print(f"INDEX[{index}]:")
        properties = mappings['properties']
        parse_field_content(properties)


if __name__ == '__main__':
    with open('field_table.csv', 'w', newline='') as csvfile, open('mappings_export_hzxy_ent.json', 'r',
                                                                   encoding='utf-8') as jsonfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Field', 'Type', 'Ignore Above', 'Analyzer'])
        main(jsonfile)
