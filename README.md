# ElasticSearchHelper

一个用于ElasticSearch运维的命令行工具集。

## 安装

### 从PyPI安装（推荐）
```bash
pip install eshelper
```

### 从源码安装
```bash
git clone https://github.com/Haoke98/ElasticSearchHelper.git
cd ElasticSearchHelper
pip install -e .
```

## 环境配置

在使用之前，需要配置ElasticSearch连接信息。你可以通过以下两种方式之一配置环境变量：

1. 在环境变量文件(.env)中配置：
```bash
SLRC_ES_PROTOCOL=https
SLRC_ES_HOST=your-es-host:9200
SLRC_ES_USERNAME=your-username
SLRC_ES_PASSWORD=your-password
SLRC_ES_CA=/path/to/ca.crt  # 如果使用https
```

2. 使用 `-e` 选项指定环境变量文件：
```bash
es-helper -e /path/to/your.env <command>
```

> 注意：如果不指定环境变量文件，程序会自动寻找当前目录下的 `.env` 文件。

## 命令行使用

安装后可以使用`es-helper`命令，支持以下功能：

### 全局选项

- `-e, --env-file`: 指定环境变量文件路径，可用于所有子命令

### 1. 查看任务进度

```bash
es-helper task --id "task-id"
```

输出示例：
```text
状态：正在运行.....
已运行：0:58:30.512872
进度：23.07%
速率：73.16/秒
预估完成于：2024-12-09 22:38:21
```

### 2. 生成索引映射

从CSV文件生成ES索引mapping：

```bash
es-helper generate-map -i fields.csv --obj2nested
```

支持的选项：
- `-i, --input`: 输入CSV文件路径（必需）
- `-o, --output`: 输出JSON文件路径（可选）
- `--obj2nested`: 将object类型转换为nested类型
- `-f, --full`: 生成完整模式的mapping

### 3. 导出字段表

从ES索引导出字段列表：

```bash
es-helper export-field-table -i index-name -gsm
```

选项说明：
- `-i, --index`: ES索引名称
- `-d, --export_dir`: 导出目录
- `-gsm, --guess-meaning`: 使用LLM推测字段含义

### 4. 重建索引

根据映射关系重建索引：

```bash
es-helper reindex -s source-index -d dest-index -m mapping.csv
```

参数说明：
- `-s, --source`: 源索引名称
- `-d, --destination`: 目标索引名称
- `-m, --mapping`: 字段映射文件（CSV格式）
- `-b, --batch-size`: 批处理大小（默认1000）
- `--strict`: 严格模式，只映射在映射表中定义的字段

## 许可证

Apache License 2.0
