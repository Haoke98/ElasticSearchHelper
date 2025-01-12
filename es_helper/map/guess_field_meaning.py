# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/7
@Software: PyCharm
@disc:
======================================="""
import csv

import ollama

# client = ollama.Client(host="http://10.2.1.0:51434")
client = ollama.Client(host="https://ollama.0p.fit")


def guess_field_meaning(field_name):
    """分析描述并打标签"""
    messages = [
        {
            "role": "system",
            "content": '''
                        请根据用户所给的ElasticSearch上某一个企业相关信息索引的字段名称, 猜测出其实际含义 
                        输出格式要求:
                        1. 必须是中文的.
                        2. 不要有其他额外的叙述语句和其他内容.
                        3. 不要有其他额外符号.
                '''
        },
        {
            'role': 'user',
            'content': field_name
        }
    ]
    while True:
        response = client.chat(
            model='mistral-nemo',
            messages=messages
        )
        if "\n" not in response.message.content and "\r" not in response.message.content and "\t" not in response.message.content and len(
                response.message.content) < 100:
            return response.message.content
        print(f"推理异常(文本长度为:{len(response.message.content)}), 正在触发重新推理....")
        messages.append({
            "role": "assistant",
            "content": response.message.content
        })
        messages.append({
            "role": "user",
            "content": "不对, 请你重新猜一下"
        })


def generate_meaning_guessed_field_table(ordinary_field_table_fp: str, meaning_guessed_field_table_fp: str,
                                         field_full_name_col_index: int = 0):
    with open(meaning_guessed_field_table_fp, 'w', encoding='utf-8') as fw, open(ordinary_field_table_fp, 'r',
                                                                                 encoding='utf-8') as fr:
        writer = csv.writer(fw)
        reader = csv.reader(fr)
        header = next(reader) + ["Meaning"]
        writer.writerow(header)
        for i, row in enumerate(reader, 1):
            field_full_name = row[field_full_name_col_index]
            print(i, field_full_name, end=': ')
            meaning = guess_field_meaning(field_full_name)
            print(meaning)
            writer.writerow(row + [meaning])


if __name__ == '__main__':
    generate_meaning_guessed_field_table('field-table.csv', 'field-table-meaning-guessed.csv')
