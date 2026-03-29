# S-Matrix 设计文档 v2（已归档，仅供历史参考）

> **⚠️ 归档声明**：本文件已不再维护，内容可能与当前计划不一致。
> 当前权威文档：`.plans/task_plan.md`、`.plans/progress.md`、`.plans/findings.md`
> 归档时间：2026-03-27
> 更新时间：2026-03-27
> 版本：v2.0（自进化多Agent架构设计）

---

## 一、项目概述

**S-Matrix**（又名 Doris 数据中台）是一个基于 **Apache Doris 4.0** 的智能数据中台，目标是让用户用自然语言查询数据库。

**核心问题域**：
- 模糊匹配（"广州" → 匹配 "广州市"）
- 多维度组合查询（年份 + 地域 + 收入范围）
- 跨表 JOIN 查询
- 动态适配新上传的表结构

---

## 二、技术栈（实际）

### 后端
- Python 3.11
- FastAPI 0.115.0
- Apache Doris 4.0.0 — MPP 实时分析数据库
- Vanna.AI 0.7.9 — Text-to-SQL 骨架（RAG 部分当前未启用）
- PyMySQL 1.1.1
- Pandas 2.2.3
- APScheduler 3.10.4

### 前端
- Vue 3.5.22 + TypeScript
- Vite 7.1.7
- Ant Design Vue 4.2.6

### 部署（实际端口）
| 服务 | 容器内端口 | 宿主机端口 |
|------|-----------|-----------|
| Frontend (Nginx) | 80 | **35173** |
| Backend (FastAPI) | 8000 | **38018** |
| Doris FE | 8030/9030 | **38030/39030** |
| Doris BE | 8040/9050 | **38040/39050** |
| 网络段 | — | 192.168.100.0/24 |

### AI 服务
- DeepSeek API / OpenAI API（OpenAI-compatible）
- 模型：`deepseek-chat`（可配置）
- 当前 API Key 存储在 docker-compose.yml 环境变量中（安全隐患，待修复）

---

## 三、目录结构（实际）

```
S-Matrix/
├── doris-api/                  # 后端服务
│   ├── main.py                # FastAPI 主程序 (~900行)
│   ├── vanna_doris.py         # Vanna.AI 集成 + Prompt 工程
│   ├── db.py                  # Doris 数据库客户端
│   ├── handlers.py            # Doris AI_GENERATE() 调用处理器
│   ├── datasource_handler.py  # 外部数据源同步 (APScheduler)
│   ├── upload_handler.py      # Excel 上传处理 (Stream Load)
│   ├── metadata_analyzer.py   # 上传后自动分析表结构
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── doris-frontend/             # 前端服务
│   └── src/components/        # 注意：是 components/ 不是 views/
│       ├── ExcelUpload.vue
│       ├── LLMConfig.vue      # 注意：不是 AIConfig.vue
│       ├── NaturalQuery.vue   # 注意：不是 AIQuery.vue
│       ├── DataQuery.vue
│       ├── DataSourceSync.vue
│       └── TableRegistry.vue
├── doris-source/              # 空目录（数据源挂载点）
├── docker-compose.yml
├── API_EXAMPLES.md
├── test_prompt_preview.py     # 根目录测试文件
├── init.sh / init.ps1
└── update.sh / update.ps1
```

---

## 四、Vanna.AI 在本项目中的真实角色分析

### 4.1 当前实际使用情况

Vanna.AI 的设计是一个 RAG-based Text-to-SQL 框架，包含：
1. **存储层**：存 DDL、文档、历史 Q→SQL 对
2. **检索层**：向量检索相似问题
3. **生成层**：把检索到的上下文组装 Prompt，调用 LLM

**但当前项目中，存储层和检索层全部 stub 掉了：**

```python
def add_ddl(self, ddl, **kwargs) -> str:
    return "DDL storage not implemented"   # 没有存储

def generate_embedding(self, data, **kwargs) -> List[float]:
    return []                               # 没有向量化

def get_similar_question_sql(self, question, **kwargs):
    return []                               # 没有检索
```

**实际上只用了 Vanna 的骨架**，真正工作全在 `get_sql_prompt()` 里：
- 用关键词匹配从 `information_schema` 获取相关表 DDL
- 手动查 sample 数据注入 Prompt
- 手写模糊匹配规则注入 Prompt

### 4.2 核心结论

> 当前：Vanna = 一个结构化的 Prompt 组装框架
> 目标：Vanna = 一个带记忆的自学习 Text-to-SQL 系统

**机会点**：激活 Vanna 的 RAG 能力（存储历史 Q→SQL + 检索），系统就能从每次查询中持续学习。

---

## 五、目标架构 v2：S-Matrix 自进化查询引擎

### 5.1 设计思想

借鉴 OpenClaw/ReAct/Reflexion 的核心思想：**Plan → Execute → Reflect → Learn 循环**。

每次查询不仅是一次回答，也是一次学习机会：
- 成功的 Q→SQL 对存入知识库（主动学习）
- 失败的 SQL 经过反思修复后也存入（被动学习）
- Meta-Agent 在新表上传时自动分析并持久化字段语义

### 5.2 架构图

```
用户上传 Excel
      │
      ▼
Meta-Agent (自动触发)
  · 分析表头 + 前10行样本数据
  · LLM 推断每列语义（地理/时间/财务/文本...）
  · 提取枚举值（城市列、类型列等分类字段）
  · 生成 Table Admin Agent 配置
  · 存储到 _sys_table_agents + _sys_field_catalog
      │
      ▼
Knowledge Base (Doris 系统表)
  ├── _sys_table_agents      — 每张表的 Agent 配置 (字段语义+匹配规则+CoT模板)
  ├── _sys_query_history     — 成功的 Q→SQL 对 (RAG训练数据，自进化核心)
  └── _sys_field_catalog     — 字段枚举值缓存 (模糊匹配基础)

用户提问
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                   查询处理流水线                      │
│                                                     │
│  Step 1: RAG 检索                                    │
│  · 从 _sys_query_history 检索相似历史 Q→SQL          │
│  · 作为 Few-shot 示例注入后续 Prompt                 │
│                                                     │
│  Step 2: Planner Agent (调度员)                     │
│  · CoT 意图识别（问的是什么？涉及哪些维度？）          │
│  · 路由到相关的 Table Admin Agents                   │
│  · 输出结构化计划：{表, 过滤条件, 聚合方式}            │
│                                                     │
│  Step 3: Table Admin Agents (表管理员)               │
│  · 每张表一个 Agent 配置，带专属 CoT 模板             │
│  · 注入字段枚举值（来自 _sys_field_catalog）          │
│  · 注入相似历史 Q→SQL（来自 Step 1 RAG 检索）        │
│  · 生成高精度 SQL                                    │
│                                                     │
│  Step 4: Coordinator Agent (汇总员)                 │
│  · 跨表时合并 SQL（JOIN 或应用层聚合）               │
│  · 单表时直接透传                                    │
│                                                     │
│  Step 5: 执行 + 验证                                │
│  · 执行 SQL                                        │
│  · 成功 → 写入 _sys_query_history（自进化！）        │
│  · 失败 → Auto-Repair Agent（反思重写）→ 再执行      │
│          → 修复成功同样写入历史                      │
└─────────────────────────────────────────────────────┘
      │
      ▼
返回结果给用户
```

### 5.3 自进化的三个机制

| 机制 | 实现方式 | 效果 |
|------|---------|------|
| **查询记忆** | 成功执行的 Q→SQL 写入 `_sys_query_history`；下次相似问题关键词检索作为 Few-shot 注入 | 越用越准，同类问题不再犯错 |
| **Meta-Agent 自学习** | 新表上传时自动更新 `_sys_field_catalog`（字段枚举值、数值范围） | 新数据自动适配，枚举值始终最新 |
| **Auto-Repair** | SQL 执行报错 → 错误信息 + 原 SQL 喂回 LLM 重写 → 修复成功后写入历史 | 错误变成训练数据，自动消除同类错误 |

### 5.4 与 Vanna 的关系

新架构**不抛弃 Vanna**，而是激活它原本设计但未实现的部分：

| Vanna 方法 | 当前状态 | v2 实现 |
|-----------|---------|---------|
| `add_question_sql()` | stub，返回字符串 | 写入 `_sys_query_history` |
| `get_similar_question_sql()` | 返回 [] | 从 `_sys_query_history` 关键词全文检索 |
| `generate_embedding()` | 返回 [] | Phase 5 可选：接入向量库后激活 |
| `get_related_ddl()` | 关键词匹配 | 保留，增加 agent_config 辅助权重 |
| `get_sql_prompt()` | 手写规则 | 升级：注入 Few-shot + 枚举值 + CoT 模板 |

---

## 六、数据库 Schema 设计（新增系统表）

### 6.1 `_sys_table_agents`（表管理员配置）

```sql
CREATE TABLE `_sys_table_agents` (
    `table_name`   VARCHAR(255) NOT NULL,
    `agent_config` TEXT NOT NULL,
    `created_at`   DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_at`   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP DUPLICATE KEY(`table_name`);
```

`agent_config` JSON 结构示例：
```json
{
  "table_description": "环保机构数据，50+维度",
  "fields": {
    "所在省": { "semantic": "geographic-province", "match": "fuzzy", "values": ["广东省", "北京市"] },
    "所在市": { "semantic": "geographic-city", "match": "fuzzy", "values": ["广州市", "深圳市"] },
    "成立年": { "semantic": "temporal-year", "match": "exact_or_range", "range": [1990, 2024] },
    "2021总收入": { "semantic": "financial-income", "match": "range", "unit": "元" }
  },
  "cot_template": "1.识别查询维度 2.为每个维度确定匹配策略 3.生成WHERE子句 4.添加聚合",
  "query_examples": []
}
```

### 6.2 `_sys_query_history`（查询历史 / RAG训练数据）

```sql
CREATE TABLE `_sys_query_history` (
    `id`          BIGINT,
    `question`    TEXT NOT NULL,
    `sql`         TEXT NOT NULL,
    `table_names` VARCHAR(1000),
    `success`     BOOLEAN DEFAULT TRUE,
    `row_count`   INT,
    `created_at`  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP DUPLICATE KEY(`id`);
```

### 6.3 `_sys_field_catalog`（字段枚举值缓存）

```sql
CREATE TABLE `_sys_field_catalog` (
    `table_name`  VARCHAR(255) NOT NULL,
    `field_name`  VARCHAR(255) NOT NULL,
    `field_type`  VARCHAR(50),
    `enum_values` TEXT,
    `value_range` VARCHAR(200),
    `updated_at`  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP DUPLICATE KEY(`table_name`, `field_name`);
```

---

## 七、各 Agent 核心 Prompt 设计

### 7.1 Meta-Agent Prompt

```
你是数据表结构分析专家。请分析以下表格，生成"表管理员Agent"配置。

表名: {table_name}
列名: {columns}
样本数据(前10行): {sample_data}

请输出 JSON，包含：
1. table_description: 表的业务描述（一句话）
2. fields: 每个字段的语义类型(geographic/temporal/financial/categorical/text/id)
   和匹配策略(exact/fuzzy/range/like)
   对于 categorical 字段，从样本提取 values 列表
3. cot_template: 处理此表查询的推理步骤模板

只输出 JSON，不要其他文字。
```

### 7.2 Planner Agent Prompt

```
你是SQL查询规划员。分析用户问题，识别涉及的表，拆分为子任务。

可用表：
{table_descriptions}

用户问题：{question}

推理步骤：
1. 核心目标（统计/列举/比较/排名）
2. 需要哪些字段，在哪些表
3. 是否需要跨表
4. 子任务拆分

输出 JSON：{"intent": "...", "tables": [...], "subtasks": [...], "needs_join": bool}
```

### 7.3 Table Admin Agent Prompt 结构

```
[DDL — 表结构]
[字段语义和枚举值 — 来自 agent_config]
[历史相似查询 — 来自 RAG 检索的 Q→SQL 示例]
[CoT 模板 — 来自 agent_config]
[当前子任务]
[规则：只输出 SQL，用 LIKE 处理模糊匹配，不加 markdown]
```

### 7.4 Auto-Repair Agent Prompt

```
以下 SQL 执行失败，请分析错误原因并修正。

原始问题：{question}
失败 SQL：{failed_sql}
错误信息：{error_message}
表结构：{ddl}

请输出修正后的 SQL（只输出 SQL，不要解释）。
```

---

## 八、实现路线图

### Phase 1：激活 Vanna RAG（2-3天）— 最高优先级
**最小改动，最大收益。**

- [ ] 创建 `_sys_query_history` 表
- [ ] 实现 `VannaDoris.add_question_sql()`：写入历史
- [ ] 实现 `VannaDoris.get_similar_question_sql()`：全文检索
- [ ] 在 `main.py` 查询成功后调用 `add_question_sql()`
- [ ] 在 `get_sql_prompt()` 中注入检索到的历史示例

**预期效果**：系统开始记忆，同类问题第二次必然更准。

### Phase 2：Meta-Agent 升级（3-4天）
- [ ] 创建 `_sys_table_agents` 和 `_sys_field_catalog` 表
- [ ] 扩展 `metadata_analyzer.py` 输出 agent_config JSON
- [ ] 上传接口触发 Meta-Agent 并存储配置
- [ ] `get_sql_prompt()` 注入 agent_config（枚举值 + CoT 模板）

**预期效果**：新表上传后 SQL 精度大幅提升，无需手动维护匹配规则。

### Phase 3：多 Agent 编排（5-7天）
- [ ] 实现 Planner Agent（表路由 + 子任务拆分）
- [ ] 实现 Table Admin Agent（使用 agent_config 生成 SQL）
- [ ] 实现 Coordinator Agent（跨表结果合并）
- [ ] 修改 `/api/query/natural` 使用新流水线
- [ ] （可选）引入 LangGraph 做 Agent 编排

### Phase 4：Auto-Repair + 生产加固（3-4天）
- [ ] 实现 Auto-Repair Agent
- [ ] 连接池（PyMySQL connection pool）
- [ ] DDL + 枚举值 TTL 缓存
- [ ] 安全修复（移除 .env、限制 CORS、API 认证）

### Phase 5（可选）：向量 RAG
- [ ] 接入 Milvus 或 Doris 向量索引
- [ ] 激活 `generate_embedding()`
- [ ] 语义相似度检索替代关键词检索

---

## 九、API 接口清单

### 已实现（宿主机端口 38018）

| 方法 | 路径 | 功能 | 状态 |
|------|------|------|------|
| GET | `/api/health` | 健康检查 | ✅ |
| POST | `/api/execute` | 统一 AI 操作（query/sentiment/classify 等） | ✅ |
| GET | `/api/tables` | 列出所有表 | ✅ |
| GET | `/api/tables/{name}/schema` | 获取表结构 | ✅ |
| POST | `/api/llm/config` | 创建 LLM 配置 | ✅ |
| GET | `/api/llm/config` | 列出 LLM 配置 | ✅ |
| DELETE | `/api/llm/config/{name}` | 删除 LLM 配置 | ✅ |
| POST | `/api/llm/config/{name}/test` | 测试 LLM 配置 | ✅ |
| POST | `/api/query/natural` | **自然语言查询（核心）** | ✅ |
| POST | `/api/upload` | Excel 上传 | ✅ |
| POST | `/api/upload/preview` | Excel 预览 | ✅ |
| POST | `/api/datasource` | 保存数据源 | ✅ |
| GET | `/api/datasource` | 列出数据源 | ✅ |
| POST | `/api/datasource/{id}/sync` | 同步数据源表 | ✅ |
| GET | `/api/datasource/{id}/tables` | 获取数据源表列表 | ✅ |

### 规划中（v2）

| 方法 | 路径 | 功能 | 阶段 |
|------|------|------|------|
| GET | `/api/agents` | 列出所有表 Agent 配置 | Phase 2 |
| GET | `/api/agents/{table}` | 获取表 Agent 配置 | Phase 2 |
| POST | `/api/agents/{table}/refresh` | 手动刷新表 Agent | Phase 2 |
| GET | `/api/query/history` | 查询历史列表 | Phase 1 |
| POST | `/api/query/history/{id}/feedback` | 用户标记 SQL 对/错 | Phase 1 |

---

## 十、问题与风险

### 安全（P0 — 立即修复）
- `.env` 文件提交到仓库（含 API Key）→ 删除，改用 docker secret 或 .gitignore
- CORS `allow_origins=["*"]` → 限制到实际前端域名
- 无 API 认证 → 添加 Bearer Token

### 性能（P1）
- 无连接池 → 每次请求新建连接，高并发崩溃
- DDL 每次实时查 `information_schema` → 加 TTL 缓存
- 枚举值每次 DISTINCT 查询 → 写入 `_sys_field_catalog` 后复用

### 代码质量（P2）
- 无单元测试
- `print()` 和 `logging` 混用
- `main.py` 约 900 行，部分功能与 `handlers.py` 有重叠

---

## 十一、关键设计决策

**为什么不直接用 LangGraph？**
Phase 1/2 完全不需要，过早引入增加复杂度。先实现核心自学习，多 Agent 协作逻辑稳定后再迁移。

**为什么用 Doris 存 RAG 训练数据？**
当前数据量小，Doris 全文检索足够。避免引入额外依赖（Milvus/Redis）。等数据量大后可无缝升级到向量检索。

**Vanna 是否要替换？**
不替换，激活它原本设计但 stub 掉的部分。骨架是对的，只需要实现存储和检索层。

---

## 十二、联系方式

- **仓库**：https://github.com/sga-jerrylin/S-Matrix
- **分支**：master
- **联系**：jerrylin@sologenai.com
