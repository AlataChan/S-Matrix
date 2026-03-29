# S-Matrix Findings

> 创建时间：2026-03-27
> 来源：代码库深度分析 + 原始交接文档

本文记录对代码库的实际分析发现，包括架构事实、踩坑点、和关键设计决策的依据。
**所有结论基于实际读取代码，非假设。**

---

## 1. 项目基本事实

| 属性 | 实际值 | 原文档描述 | 差异 |
| ---- | ------ | --------- | ---- |
| 前端端口（宿主机） | 35173 | 5173 | 不同 |
| 后端端口（宿主机） | 38018 | 8018 | 不同 |
| Doris FE 端口（宿主机） | 38030 | 18030 | 不同 |
| Doris BE 端口（宿主机） | 38040 | 18040 | 不同 |
| 前端组件路径 | `src/components/` | `src/views/` | 不同 |
| LLM 配置组件 | `LLMConfig.vue` | `AIConfig.vue` | 不同 |
| 自然语言查询组件 | `NaturalQuery.vue` | `AIQuery.vue` | 不同 |
| 网络段 | 192.168.100.0/24 | 192.168.88.0/24 | 不同 |
| A2A 架构 | 未实现，仅规划 | "已采用" | 严重夸大 |

---

## 2. Vanna.AI 实际使用情况（关键发现）

### 2.1 RAG 功能全部 stub

`doris-api/vanna_doris.py` 中以下方法全部返回空值，RAG 完全未激活：

```python
def add_ddl(self, ddl, **kwargs) -> str:
    return "DDL storage not implemented"        # ← stub

def add_documentation(self, documentation, **kwargs) -> str:
    return "Documentation storage not implemented"  # ← stub

def add_question_sql(self, question, sql, **kwargs) -> str:
    return "Question-SQL storage not implemented"   # ← stub

def get_training_data(self, **kwargs) -> Any:
    return []                                   # ← stub

def generate_embedding(self, data, **kwargs) -> List[float]:
    return []                                   # ← stub

def get_similar_question_sql(self, question, **kwargs) -> List:
    return []                                   # ← stub（关键！）
```

**影响**：系统没有任何历史记忆，每次查询从零开始，无法积累经验。

### 2.2 真正在工作的部分

真正发挥作用的是 `get_sql_prompt()` 方法（约 100 行），它：

1. 实时查 `information_schema.COLUMNS` 获取 DDL
2. 实时查 `SELECT * FROM table LIMIT 3` 获取样本数据
3. 实时查 `SELECT DISTINCT column LIMIT 20` 获取枚举值
4. 将以上内容手工拼接成 Prompt

另外 `auto_fuzzy_match_locations()` 用正则表达式后处理生成的 SQL，将地理字段的精确匹配强制改为 LIKE。

### 2.3 性能隐患

每次自然语言查询都会触发：

- 1 次 `information_schema` 全量扫描
- N 次 `SELECT * FROM table LIMIT 3`（N = 相关表数量）
- M 次 `SELECT DISTINCT column LIMIT 20`（M = 重要列数量）

无任何缓存，高并发下会造成 Doris 压力。

---

## 3. 依赖环境（已验证）

```text
Python 环境（宿主机）：
  vanna == 0.7.9
  chromadb == 1.3.4（已安装，底层 SQLite 3.46.0）
  DuckDB：未安装

容器内（doris-api）依赖（requirements.txt）：
  fastapi==0.115.0
  uvicorn[standard]==0.32.0
  pymysql==1.1.1
  pandas==2.2.3
  openpyxl==3.1.5
  python-multipart==0.0.12
  pydantic==2.9.2
  cryptography==43.0.1
  requests==2.32.3
  vanna==0.7.9
  openai==1.54.5
  apscheduler==3.10.4

注意：requirements.txt 中没有 chromadb，需要 Phase 5 时添加。
```

---

## 4. 代码质量发现

### 4.1 main.py 规模问题

`doris-api/main.py` 约 900 行，承担了过多职责：

- FastAPI 路由定义
- Doris 初始化逻辑（`_init_doris_sync`，约 80 行）
- 自然语言查询逻辑（`natural_language_query`，约 90 行）
- 元数据分析触发逻辑
- 数据源管理路由

建议 Phase 3 重构时按职责拆分。

### 4.2 安全问题

- `.env` 文件存在于仓库（含真实 API Key `sk-748638f482f74b7392a6dafd89bdd307`，可能已失效）
- CORS 配置 `allow_origins=["*"]`，任意来源均可访问
- 无任何 API 认证机制
- docker-compose.yml 中明文配置 API Key

### 4.3 日志问题

`print()` 和 `logging` 混用，无结构化日志，生产环境难以追踪问题。

### 4.4 无连接池

`db.py` 每次请求新建 PyMySQL 连接，无连接池。Doris 单 FE 连接数有上限，高并发会触发 `Too many connections`。

---

## 5. 现有系统表

`datasource_handler.py` 的 `init_tables()` 已初始化以下系统表：

- `_sys_datasources`：外部数据源配置
- `_sys_sync_tasks`：定时同步任务
- `_sys_table_registry`：表注册信息（display_name、description）
- `_sys_table_metadata`：表元数据（LLM 分析结果，已有！）

**关键发现**：`_sys_table_metadata` 已经存储 LLM 分析的表描述，但 `vanna_doris.py` 的 `get_related_documentation()` 已经在读取它（联表查询 `_sys_table_registry` + `_sys_table_metadata`）。这部分基础设施比预期更完整。

---

## 6. 前端组件实际情况

```text
doris-frontend/src/components/
├── ExcelUpload.vue      # Excel 上传，调用 /api/upload
├── LLMConfig.vue        # LLM 配置管理，调用 /api/llm/config
├── NaturalQuery.vue     # 自然语言查询，调用 /api/query/natural
├── DataQuery.vue        # 结构化查询，调用 /api/execute
├── DataSourceSync.vue   # 数据源同步，调用 /api/datasource
├── TableRegistry.vue    # 表注册管理
└── HelloWorld.vue       # 默认模板残留，无实际功能
```

---

## 7. 向量存储技术选型分析

### 7.1 为什么不用 DuckDB 做 Vanna 存储

DuckDB 在 Vanna 中的定位是**被查询的数据源**（`connect_to_duckdb()`），不是向量存储后端。Vanna 没有 DuckDB 向量存储实现。

### 7.2 ChromaDB 的中文问题

ChromaDB 默认使用 `all-MiniLM-L6-v2`（英文优化模型）。对中文问句的语义向量质量差，会导致相似问题检索失败。

如果使用 ChromaDB，必须替换为中文模型：

```python
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
ef = SentenceTransformerEmbeddingFunction(model_name="BAAI/bge-small-zh-v1.5")
```

### 7.3 最优方案：Doris 原生向量索引

Doris 4.0 支持 HNSW 向量索引（ANN 检索），可以：

- 直接存储 embedding 向量到 `_sys_query_history` 表
- 用 Doris ANN 查询做相似度检索
- **零额外服务依赖**，利用已有 Doris 基础设施

这是 Phase 5 的目标方案。Phase 1 先用 Doris 全文检索（关键词匹配）作为过渡。

---

## 8. 与 OpenClaw / ReAct / Reflexion 的映射

| 概念 | 本项目对应实现 |
| ---- | ----------- |
| Plan | Planner Agent（子任务拆分） |
| Execute | Table Admin Agent（SQL 生成 + 执行） |
| Observe | 执行结果检查（行数、错误信息） |
| Reflect | Auto-Repair Agent（错误反思重写） |
| Learn | `_sys_query_history` 写入（经验积累） |
| Memory | RAG 检索（长期记忆） + agent_config（结构化知识） |

---

## 9. 关键约束

1. **Doris 表结构限制**：中文列名需要反引号包裹，`auto_fuzzy_match_locations()` 已处理，但 Planner Agent 生成的 SQL 也需注意
2. **Stream Load API**：Excel 上传用的是 Doris HTTP Stream Load，不是 MySQL 协议，端口是 8030（容器内），宿主机 38030
3. **LLM 超时**：当前 `requests.post(..., timeout=60)`，复杂多 Agent 查询可能需要增加超时
4. **APScheduler**：定时同步任务在 API 进程内运行，重启后任务重新注册，无持久化

---

## 10. Codex Plan Review 发现（2026-03-27）

> 来源：Plan Review 评审，评审者：Codex

### 10.1 记忆污染风险（高优先级）

**问题**：原计划在 SQL 执行成功后立即写入历史，但系统基线准确率约 60%。执行成功不等于语义正确——空结果集、幻表查询、错误业务逻辑的 SQL 都可能通过执行，若写入历史会污染 Few-shot 检索。

**解决方案（已更新到 task_plan.md）**：

- `_sys_query_history` 增加 `quality_gate TINYINT` 字段（0=待审，1=通过，2=拒绝）
- 写入条件（必须同时满足）：
  1. SQL 执行无报错
  2. 返回行数 > 0
  3. 查询到的表名在 `_sys_table_registry` 中存在
- `get_similar_question_sql()` 只检索 `quality_gate=1` 的记录

### 10.2 安全问题前置（高优先级）

**问题**：P0 安全问题（硬编码 API Key、wildcard CORS）原定 Phase 4 修复，但 Phase 1 新增的历史写入接口会在无认证状态下暴露。且仓库可能公开，硬编码 Key 已是泄漏风险。

**解决方案（已更新到 task_plan.md）**：

- 提取 SEC-1 和 SEC-2 作为 Phase 1 前置必做项
- SEC-1：将 API Key 迁移到 `.env` 文件，加入 `.gitignore`
- SEC-2：CORS 收紧为 `["http://localhost:35173"]`
- 完整 API 认证仍在 Phase 4

### 10.3 跨表关系模型缺失（高优先级）

**问题**：Phase 3 目标支持 2+ 表 JOIN，但没有任何阶段定义如何发现、存储、验证表关系。对于 Excel 上传的表，Agent 只能猜测 JOIN key，极易出错。

**解决方案（已更新到 task_plan.md）**：

- 新增 `_sys_table_relationships` 系统表
- 新增 P3-T0 前置任务：关系发现（LLM 推断）+ 人工覆盖 API

### 10.4 Schema 设计问题（中优先级）

**问题**：

- `_sys_query_history` 无 `question_hash` 列，但去重计划依赖 MD5
- `_sys_table_agents`、`_sys_field_catalog` 使用 DUPLICATE KEY，但语义是每表/字段一条当前配置

**解决方案（已更新到 task_plan.md）**：

- `_sys_query_history` 增加 `question_hash VARCHAR(64)` 列
- 三张新表全部改为 `UNIQUE KEY`

### 10.5 RAG 检索集成点错误（中优先级）

**问题**：原计划在 `get_sql_prompt()` 内调用检索，但实际上 `generate_sql()` 在 line 415/433 已硬编码 `question_sql_list = []`。`get_sql_prompt()` 接收 `question_sql_list` 作为参数，已有注入逻辑。

**解决方案（已更新到 task_plan.md）**：

- P1-T4 改为：在 `generate_sql()` 中调用 `get_similar_question_sql()` 填充 `question_sql_list`
- `get_sql_prompt()` 无需修改

### 10.6 基准测试缺失（中优先级）

**问题**：90% 准确率目标无法验证，每 Phase 只有几个手动 curl 命令。

**解决方案（已更新到 task_plan.md）**：

- P1-T6 新增：创建黄金测试集 `tests/golden_queries.json`（5-10 条标准问答对）
- 每 Phase 结束时运行黄金集验证

### 10.7 `_sys_table_metadata` vs `_sys_table_agents` 分工（中优先级）

**问题**：Phase 2 新增 `_sys_table_agents` 与现有 `_sys_table_metadata` 存在功能重叠，分工未明确。

**决策（已更新到 task_plan.md）**：

- `_sys_table_metadata`（保留）= 原始 LLM 分析文本，`metadata_analyzer.py` 输出
- `_sys_table_agents`（新增）= 结构化操作配置，从 metadata 派生
- Phase 2 新增转换步骤，含 `source_hash` 检测避免重复生成
