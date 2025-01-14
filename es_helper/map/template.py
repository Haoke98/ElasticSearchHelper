# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/1/14
@Software: PyCharm
@disc: 索引模板相关操作
======================================="""
import json
import os
from elasticsearch import Elasticsearch, NotFoundError

def update_template_mapping(es_client: Elasticsearch, template_name: str, mapping_json_fp: str):
    """
    更新索引模板的mapping
    
    Args:
        es_client: Elasticsearch客户端
        template_name: 模板名称
        mapping_json_fp: mapping JSON文件路径
    """
    try:
        # 获取现有模板 - 使用新版API
        template = es_client.indices.get_index_template(name=template_name)
        template_body = template['index_templates'][0]['index_template']
        
        # 读取新的mapping
        with open(mapping_json_fp, 'r', encoding='utf-8') as f:
            new_mapping = json.load(f)
        
        # 更新模板的mapping部分
        template_body['template']['mappings'] = new_mapping
        
        # 更新模板 - 使用新版API
        response = es_client.indices.put_index_template(
            name=template_name,
            body=template_body
        )
        
        if response.get('acknowledged'):
            print(f"模板 {template_name} 更新成功")
        else:
            print(f"模板 {template_name} 更新失败")
            
    except NotFoundError:
        print(f"模板 {template_name} 不存在")
    except Exception as e:
        print(f"更新模板时发生错误: {str(e)}") 