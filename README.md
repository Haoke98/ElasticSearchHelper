# ElasticSearchHelper

## 脚本&功能

* `map/generate-map.py`: 从CSV倒推索引map
* `map/generate_field_table.py`: 从map.json解析出字段表格
* `map/gues-field-meaning.py`: 基于LLM根据字段名推理出其实际意义和用处
* `health-monitoring/es-health-monitoring`: 实时监控ES集群健康状态
* `health-monitoring/check-analyzer-health.py`: 挨个节点检查集群中的IK分词器能否正常执行分词任务
* `task/main.py`: 按百分比展示任务的进度,并估算出预计需要时间和预计完成时间. 