# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/12
@Software: PyCharm
@disc:
======================================="""
import os

# ES 内部限定, 无法修改
TYPE_NESTED_MAX_LIMIT = 50
USER_HOME = os.path.expanduser("~")
APP_HOME_DIR = os.path.join(USER_HOME, "ElasticSearchHelper")
EXPORT_DIR = os.path.join(APP_HOME_DIR, 'export')
DEFAULT_MAP = {
            "_routing": {
                "required": False
            },
            "dynamic": "strict",
            "_source": {
                "excludes": [],
                "includes": [],
                "enabled": True
            },
            "dynamic_templates": [],
            "properties": {}
        }
