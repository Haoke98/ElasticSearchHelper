# ElasticSearchHelper

## 脚本&功能

* `map/generate-map.py`: 从CSV倒推索引map
* `map/generate_field_table.py`: 从map.json解析出字段表格
* `map/gues-field-meaning.py`: 基于LLM根据字段名推理出其实际意义和用处
* `health-monitoring/es-health-monitoring`: 实时监控ES集群健康状态

secret.py

```python
class ES:
    host = "https://ip:port"
    username: str = "username"
    password: str = "password"
    CA_CERTS: str = "/usr/local/certs/http_ca.crt"

```