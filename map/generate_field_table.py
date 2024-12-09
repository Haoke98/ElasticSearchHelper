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
import os

from elasticsearch import Elasticsearch

from map.guess_field_meaning import generate_meaning_guessed_field_table


def generate_field_table(map_json_fp: str, field_table_fp: str):
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
            level = len(final_field_name.split("."))
            writer.writerow([final_field_name, field_type, ignore_above, analyser, level, field_name])
            if field_type == "object" and dict(field_map).__contains__("properties"):
                parse_field_content(field_map["properties"], final_field_name)
            if dict(field_map).__contains__("fields"):
                parse_field_content(field_map["fields"], final_field_name)

    with open(field_table_fp, 'w', newline='') as csvfile, open(map_json_fp, 'r', encoding='utf-8') as jsonfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Field Full Name', 'Type', 'Ignore Above', 'Analyzer', 'Level', 'Field Name'])
        data: dict = json.load(jsonfile)
        for index, _data in data.items():
            mappings = _data["mappings"]
            firstProperties = mappings['properties']
            parse_field_content(firstProperties)


def export_field_table(index: str):
    EsClient = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv("SLRC_ES_HOST"),
                             http_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                             ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
    resp = EsClient.indices.get_mapping(index=index)
    map_json_fp = f'index-map-exported-{index}.json'
    with open(map_json_fp, 'w', encoding='utf-8') as jsonfile:
        json.dump(resp, jsonfile, ensure_ascii=False, indent=4)
    field_table_fp = f'index-field-table-generated-{index}.csv'
    generate_field_table(map_json_fp, field_table_fp)
    meaning_guessed_field_table_fp = f"index-meaning-guessed-field-table-generated-{index}.csv"
    generate_meaning_guessed_field_table(field_table_fp, meaning_guessed_field_table_fp)
