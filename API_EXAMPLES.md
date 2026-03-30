# S-Matrix API 调用示例

本文档提供通过 HTTP 访问 S-Matrix 数据中台的完整示例。

**API 基础地址**: `http://localhost:38018`

> **认证说明**：所有 `/api/*` 接口（除 `/api/health`）都需要在 Header 中携带 `SMATRIX_API_KEY`。
> 两种方式任选其一：`X-API-Key: <key>` 或 `Authorization: Bearer <key>`

---

## 目录

1. [健康检查](#1-健康检查)
2. [自然语言查询](#2-自然语言查询)
3. [Excel 数据上传](#3-excel-数据上传)
4. [数据查询](#4-数据查询)
5. [表管理](#5-表管理)
6. [LLM 配置管理](#6-llm-配置管理)
7. [查询历史与反馈](#7-查询历史与反馈)
8. [表关系管理](#8-表关系管理)
9. [完整 Agent 调用示例](#9-完整-agent-调用示例)

---

## 1. 健康检查

### 1.1 基础健康检查（无需认证）

```bash
curl http://localhost:38018/
```

**响应:**

```json
{
  "service": "Doris API Gateway",
  "status": "running",
  "version": "1.0.0"
}
```

### 1.2 Doris 连接检查（无需认证）

```bash
curl http://localhost:38018/api/health
```

**响应:**

```json
{
  "success": true,
  "doris_connected": true,
  "message": "Doris connection OK"
}
```

---

## 2. 自然语言查询

**核心接口**：多 Agent 流水线（Planner → TableAdmin → Coordinator → RepairAgent）

### 2.1 基本查询

```bash
curl -X POST http://localhost:38018/api/query/natural \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "query": "2022年的机构中来自于广东的有多少个?"
  }'
```

### 2.2 指定目标表（缩小查询范围）

```bash
curl -X POST http://localhost:38018/api/query/natural \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "query": "每个城市的机构数量占比是多少?",
    "table_names": ["institutions_2022"]
  }'
```

### 2.3 指定 LLM 资源

```bash
curl -X POST http://localhost:38018/api/query/natural \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "query": "统计每个省份的机构数量",
    "resource_name": "my_deepseek"
  }'
```

### 2.4 Python 示例

```python
import requests

API_KEY = "your-secret-api-key"
BASE_URL = "http://localhost:38018"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

response = requests.post(
    f"{BASE_URL}/api/query/natural",
    headers=HEADERS,
    json={"query": "统计每个省份的机构数量"}
)

result = response.json()
print(f"生成的 SQL: {result['sql']}")
print(f"查询结果: {result['data']}")
print(f"记录数: {result['count']}")
```

### 2.5 响应示例

```json
{
  "success": true,
  "query": "2022年的机构中来自于广东的有多少个?",
  "sql": "SELECT COUNT(*) as count FROM institutions WHERE year = 2022 AND province LIKE '%广东%'",
  "data": [{"count": 156}],
  "count": 1
}
```

---

## 3. Excel 数据上传

### 3.1 预览 Excel 文件

```bash
curl -X POST http://localhost:38018/api/upload/preview \
  -H "X-API-Key: your-secret-api-key" \
  -F "file=@/path/to/your/data.xlsx" \
  -F "rows=10"
```

### 3.2 上传并创建表

```bash
curl -X POST http://localhost:38018/api/upload \
  -H "X-API-Key: your-secret-api-key" \
  -F "file=@/path/to/your/data.xlsx" \
  -F "table_name=my_table" \
  -F "create_table=true" \
  -F "import_mode=replace"
```

`import_mode` 支持两种值：

- `replace`：覆盖同名表（重新上传修正版时使用）
- `append`：追加到现有表（列名和顺序必须完全一致）

上传完成后会自动触发 LLM 元数据分析，生成表的业务语义描述。

### 3.3 Python 示例

```python
import requests

API_KEY = "your-secret-api-key"
HEADERS = {"X-API-Key": API_KEY}

with open('data.xlsx', 'rb') as f:
    files = {'file': ('data.xlsx', f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
    data = {
        'table_name': 'institutions',
        'create_table': 'true',
        'import_mode': 'replace'
    }
    response = requests.post(
        "http://localhost:38018/api/upload",
        headers=HEADERS,
        files=files,
        data=data
    )

result = response.json()
print(f"上传成功: {result['success']}")
print(f"导入行数: {result['rows_imported']}")
```

### 3.4 响应示例

```json
{
  "success": true,
  "table": "institutions",
  "rows_imported": 1500,
  "table_existed": false,
  "table_created": true,
  "import_mode": "replace"
}
```

---

## 4. 数据查询

### 4.1 执行 SQL 查询

```bash
curl -X POST http://localhost:38018/api/execute \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "action": "query",
    "params": {
      "sql": "SELECT * FROM institutions LIMIT 10"
    }
  }'
```

### 4.2 支持的 action 类型

| action | 说明 |
| ------ | ---- |
| `query` | 普通 SQL 查询 |
| `sentiment` | 情感分析 |
| `classify` | 文本分类 |
| `extract` | 信息提取 |
| `stats` | 统计分析 |
| `similarity` | 语义相似度 |
| `translate` | 文本翻译 |
| `summarize` | 文本摘要 |
| `mask` | 敏感信息脱敏 |
| `fixgrammar` | 语法纠错 |
| `generate` | 内容生成 |
| `filter` | 布尔过滤 |

### 4.3 获取查询目录（业务语义视图）

```bash
curl -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/query/catalog
```

---

## 5. 表管理

### 5.1 获取所有表

```bash
curl -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/tables
```

### 5.2 获取表结构

```bash
curl -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/tables/institutions/schema
```

### 5.3 获取表注册表（含元数据）

```bash
curl -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/table-registry
```

### 5.4 删除表（含完整清理）

```bash
curl -X DELETE \
  -H "X-API-Key: your-secret-api-key" \
  "http://localhost:38018/api/table-registry/my_table?drop_physical=true&cleanup_history=true"
```

该接口会：

- 删除 Doris 物理表
- 删除 `_sys_table_registry` 中的记录
- 删除表元数据、Agent 配置、字段目录、表关系
- （可选）清理与该表相关的历史问答样本

---

## 6. LLM 配置管理

### 6.1 创建 LLM 配置

```bash
curl -X POST http://localhost:38018/api/llm/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "resource_name": "my_deepseek",
    "provider_type": "deepseek",
    "endpoint": "https://api.deepseek.com/chat/completions",
    "model_name": "deepseek-chat",
    "api_key": "sk-your-api-key"
  }'
```

### 6.2 获取所有 LLM 配置

```bash
curl -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/llm/config
```

### 6.3 测试 LLM 配置

```bash
curl -X POST \
  -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/llm/config/my_deepseek/test
```

### 6.4 删除 LLM 配置

```bash
curl -X DELETE \
  -H "X-API-Key: your-secret-api-key" \
  http://localhost:38018/api/llm/config/my_deepseek
```

---

## 7. 查询历史与反馈

### 7.1 获取查询历史

```bash
curl -H "X-API-Key: your-secret-api-key" \
  "http://localhost:38018/api/query/history?limit=50"
```

### 7.2 标记查询质量（用于 RAG 训练）

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{"quality_gate": 1}' \
  http://localhost:38018/api/query/history/<query_id>/feedback
```

`quality_gate` 值：`1` = 好，`-1` = 差，`0` = 未标记

---

## 8. 表关系管理

跨表 JOIN 查询需要预先定义表之间的关联关系。

### 8.1 创建表关系

```bash
curl -X POST http://localhost:38018/api/relationships \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key" \
  -d '{
    "table_a": "orders",
    "column_a": "customer_id",
    "table_b": "customers",
    "column_b": "id"
  }'
```

创建后，当问题涉及两张表时，CoordinatorAgent 会自动生成 JOIN SQL。

---

## 9. 完整 Agent 调用示例

```python
import requests

class SMatrixClient:
    def __init__(self, base_url="http://localhost:38018", api_key=None):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "X-API-Key": api_key or "",
            "Content-Type": "application/json"
        }

    def ask(self, question: str, table_names: list = None, resource_name: str = None):
        """自然语言查询（多 Agent 流水线）"""
        payload = {"query": question}
        if table_names:
            payload["table_names"] = table_names
        if resource_name:
            payload["resource_name"] = resource_name
        response = requests.post(
            f"{self.base_url}/api/query/natural",
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()

    def upload(self, file_path: str, table_name: str, mode: str = "replace"):
        """上传 Excel 数据"""
        headers = {"X-API-Key": self.headers["X-API-Key"]}
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{self.base_url}/api/upload",
                headers=headers,
                files={'file': f},
                data={'table_name': table_name, 'create_table': 'true', 'import_mode': mode}
            )
        response.raise_for_status()
        return response.json()

    def query_sql(self, sql: str):
        """直接执行 SQL"""
        response = requests.post(
            f"{self.base_url}/api/execute",
            headers=self.headers,
            json={"action": "query", "params": {"sql": sql}}
        )
        response.raise_for_status()
        return response.json()


# 使用示例
client = SMatrixClient(api_key="your-secret-api-key")

# 1. 上传数据
result = client.upload("institutions.xlsx", "institutions")
print(f"上传成功: {result['rows_imported']} 行")

# 2. 自然语言查询（自动多表路由 + SQL 修复）
result = client.ask("2022年广东省有多少个机构？")
print(f"SQL: {result['sql']}")
print(f"结果: {result['data']}")

# 3. 直接 SQL 查询
result = client.query_sql("SELECT * FROM institutions LIMIT 5")
print(f"查询结果: {result['data']}")
```

---

## 错误处理

所有 API 在出错时返回标准错误格式：

```json
{
  "detail": {
    "error": "错误信息",
    "traceback": "详细堆栈信息"
  }
}
```

**常见状态码**：

| 状态码 | 含义 |
| ------ | ---- |
| 401 | API Key 错误或未提供 |
| 503 | Doris 尚未就绪（启动中），等待后重试 |
| 400 | 请求参数错误 |
| 500 | 服务内部错误（查看 traceback） |

---

## API 文档

完整的交互式 API 文档（需要先通过认证）：

- **Swagger UI**: <http://localhost:38018/docs>
- **ReDoc**: <http://localhost:38018/redoc>
