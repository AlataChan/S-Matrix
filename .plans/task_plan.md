# S-Matrix Task Plan

> 创建时间：2026-03-27
> 最后更新：2026-03-28（全部 Phase 开发完成）
> 当前阶段：Completed
> 负责人：sga-jerrylin

---

## 项目目标

将 S-Matrix 从"单次查询、无记忆"升级为"自进化多 Agent 查询引擎"。

核心指标：

- 自然语言 → SQL 准确率（当前估计 ~60%，目标 >90%）
- 模糊匹配命中率（当前靠正则后处理，目标靠语义理解）
- 跨表查询支持（当前不支持，目标支持 2+ 表 JOIN）

---

## 目标架构：Plan → Execute → Reflect → Learn

```text
用户上传 Excel
      ↓
Meta-Agent（自动触发）
  · 分析表结构 → 字段语义 + 枚举值 + CoT 模板
  · 写入 Knowledge Base

用户提问
      ↓
[Step 1] RAG 检索
  · 从 _sys_query_history 检索相似历史 Q→SQL
  · Few-shot 注入后续 Prompt

[Step 2] Planner Agent
  · CoT 意图识别 → 表路由 → 子任务拆分
  · 输出：{tables, subtasks, needs_join}

[Step 3] Table Admin Agent（每表一个）
  · 注入：字段枚举值 + 匹配规则 + CoT 模板 + 历史示例
  · 输出：高精度 SQL

[Step 4] Coordinator Agent
  · 单表：直接透传
  · 多表：生成 JOIN SQL 或应用层聚合（由 Coordinator 决定）

[Step 5] 执行 + 自进化
  · 成功 → 写入 _sys_query_history（RAG 训练数据）
  · 失败 → Auto-Repair Agent → 重写 → 再写入
```

---

## 新增系统表（Doris）

```sql
-- 查询历史 / RAG 训练数据
-- id: UUID VARCHAR(36)，Python 端 str(uuid.uuid4()) 生成，INSERT 时传入
-- quality_gate: 1=通过写入, 2=手动拒绝（只写 1，通过反馈 API 降为 2）
-- question_hash: MD5(question+sql)，equality 去重；question 列走全文检索
CREATE TABLE `_sys_query_history` (
    `id`              VARCHAR(36) NOT NULL,
    `question`        TEXT NOT NULL,
    `sql`             TEXT NOT NULL,
    `table_names`     VARCHAR(1000),
    `question_hash`   VARCHAR(64),
    `quality_gate`    TINYINT DEFAULT 1,
    `is_empty_result` BOOLEAN DEFAULT FALSE,
    `row_count`       INT,
    `created_at`      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP UNIQUE KEY(`id`);
-- 去重查找（equality）：ALTER TABLE ADD INDEX idx_hash (`question_hash`) USING INVERTED;
-- 全文检索（MATCH_ANY）：ALTER TABLE ADD INDEX idx_question (`question`) USING INVERTED PROPERTIES("parser"="chinese");

-- 表管理员 Agent 配置（每表一行，可覆盖更新，派生自 _sys_table_metadata）
-- table_name 本身是自然键，无需 UUID
CREATE TABLE `_sys_table_agents` (
    `table_name`   VARCHAR(255) NOT NULL,
    `agent_config` TEXT NOT NULL,
    `source_hash`  VARCHAR(64),
    `created_at`   DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_at`   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP UNIQUE KEY(`table_name`);

-- 字段枚举值缓存（模糊匹配基础）
-- (table_name, field_name) 是自然复合键
CREATE TABLE `_sys_field_catalog` (
    `table_name`  VARCHAR(255) NOT NULL,
    `field_name`  VARCHAR(255) NOT NULL,
    `field_type`  VARCHAR(50),
    `enum_values` TEXT,
    `value_range` VARCHAR(200),
    `updated_at`  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP UNIQUE KEY(`table_name`, `field_name`);

-- 表关系目录（Phase 3 跨表 JOIN 必需）
-- id: UUID VARCHAR(36)，Python 端 str(uuid.uuid4()) 生成
CREATE TABLE `_sys_table_relationships` (
    `id`           VARCHAR(36) NOT NULL,
    `table_a`      VARCHAR(255) NOT NULL,
    `column_a`     VARCHAR(255) NOT NULL,
    `table_b`      VARCHAR(255) NOT NULL,
    `column_b`     VARCHAR(255) NOT NULL,
    `rel_type`     VARCHAR(50) DEFAULT 'foreign_key',
    `confidence`   FLOAT DEFAULT 1.0,
    `is_manual`    BOOLEAN DEFAULT FALSE,
    `created_at`   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP UNIQUE KEY(`id`);

-- Phase 5：向量列（ALTER 追加到 _sys_query_history）
-- ALTER TABLE `_sys_query_history` ADD COLUMN `question_embedding` ARRAY<FLOAT>;
-- 利用 Doris 4.0 原生 HNSW 向量索引；embedding 维度由模型决定（bge-small-zh: 512维）
```

---

## 前置条件：最小安全加固（Phase 1 前必须完成）

**原因**：Phase 1 新增历史写入接口。CORS 不等于认证——收窄 Origin 无法阻止直接服务端调用，必须部署最小 API Key 机制。

- [x] **SEC-1**：从 git 中移除硬编码密钥，并补充 `SMATRIX_API_KEY`
  - docker-compose.yml 的 `DEEPSEEK_API_KEY` 改为 `${DEEPSEEK_API_KEY}`
  - docker-compose.yml 的 `smatrix-api` 环境块新增 `SMATRIX_API_KEY=${SMATRIX_API_KEY}`
  - 将 `.env` 加入 `.gitignore`；如已提交，运行 `git rm --cached .env`
  - 提交 `.env.example`，含两个占位符：`DEEPSEEK_API_KEY=` 和 `SMATRIX_API_KEY=`

- [x] **SEC-2**：最小 API Key 认证
  - `main.py` 新增 `X-API-Key` 请求头校验，从环境变量 `SMATRIX_API_KEY` 读取
  - 未通过校验返回 `401 Unauthorized`
  - 豁免路径：`/api/health`、`/docs`、`/openapi.json`（注意是 `/api/health`，见 main.py:216）
  - ~10 行 FastAPI `Depends()` 实现，无需新依赖
  - 完整 OAuth/JWT 认证仍在 Phase 4

- [x] **SEC-3**：收紧 CORS（辅助措施，非主要防线）
  - `allow_origins=["*"]` 改为 `["http://localhost:35173"]`

---

## Phase 1：激活 Vanna RAG

**预计工期**：2-3 天
**价值**：零额外依赖，系统立刻具备自学习能力

### P1 任务清单

- [x] **P1-T1**：创建 `_sys_query_history` 表
  - 文件：`doris-api/datasource_handler.py` 的 `init_tables()`
  - Schema 见上方建表 SQL（含 `question_hash`、`quality_gate`、`is_empty_result`）
  - 同时创建 `question` 列的 INVERTED INDEX（支持 `MATCH_ANY`）
  - 验证：`SHOW TABLES LIKE '_sys_query_history'` 返回结果

- [x] **P1-T2**：实现 `VannaDoris.add_question_sql()`
  - 文件：`doris-api/vanna_doris.py`
  - ID 生成：`id = str(uuid.uuid4())`（在 Python 端生成，不依赖数据库序列）
  - 质量门（全部满足才写入）：
    1. SQL 执行无报错（有 DB 错误则不写入）
    2. 空结果合法：`is_empty_result=TRUE`，`quality_gate` 仍为 1（不因行数为 0 而拒绝）
  - 表名验证改为**软检查**（warn/log，不阻断写入）：提取 SQL 中的表名并与 `_sys_table_registry` 比对；仅记录日志，不作为硬拒绝条件（regex 对 CTE/子查询/别名不可靠）
  - 去重：`MD5(question + sql)` → 若 `question_hash` 已存在则跳过
  - 写入方式：**同步阻塞**（P1 阶段，确保验证步骤可立即读到记录）
  - `quality_gate` 默认 1；状态机迁移（1→2）由 P2-T6 的反馈 API 管理

- [x] **P1-T2b**：从已执行 SQL 提取 `table_names`（软提取，仅供存储和日志）
  - 文件：`doris-api/vanna_doris.py`（辅助函数）
  - 用 regex 解析 `FROM`/`JOIN` 子句，转为逗号分隔字符串存入 `table_names`
  - **不作为写入质量门**，仅供后续分析和日志使用

- [x] **P1-T3**：实现 `VannaDoris.get_similar_question_sql()`
  - 文件：`doris-api/vanna_doris.py`
  - 检索条件：`WHERE quality_gate=1`（包含 `is_empty_result=TRUE` 的记录）
  - 检索策略：Doris `MATCH_ANY` 全文检索（走 `question` 列 INVERTED INDEX）
  - 回退：`MATCH_ANY` 无结果时降级为 LIKE 关键词匹配
  - 返回：`[{"question": "...", "sql": "..."}]`，最多 3 条，优先非空结果

- [x] **P1-T4**：在 `generate_sql()` 中激活检索
  - 文件：`doris-api/vanna_doris.py`，line 415/433
  - 当前：`question_sql_list = []`（硬编码）
  - 修改：调用 `get_similar_question_sql(question)` 填充 `question_sql_list`
  - `get_sql_prompt()` 已有注入逻辑，无需改动
  - 同时添加 `logging.debug(f"[RAG] retrieved {len(question_sql_list)} examples")` 供验证

- [x] **P1-T5**：在查询成功后调用 `add_question_sql()`
  - 文件：`doris-api/main.py` 的 `/api/query/natural`
  - 位置：SQL 执行完毕（无论行数是否为 0），通过质量门后同步写入

- [x] **P1-T6**：创建黄金测试集 + 最小运行脚本
  - 文件：`doris-api/tests/golden_queries.json`（标准问答对）
  - 文件：`doris-api/tests/run_golden.py`（运行脚本，调用 API 并断言结果）
  - 格式（三维断言，缺一不可）：

    ```json
    {
      "question": "广州有多少机构？",
      "expected_sql_pattern": "COUNT|count",
      "expected_min_rows": 1,
      "expected_result_contains": ["广州"]
    }
    ```

    - `expected_sql_pattern`：生成 SQL 必须匹配的 regex（检测关键子句）
    - `expected_min_rows`：结果行数下限（≥0，允许 0 用于空结果测试）
    - `expected_result_contains`：结果数据中必须出现的值列表（防止语义错误的 SQL 通过行数检查）
  - 条数：5-10 条，覆盖单表聚合、模糊匹配、空结果三类
  - 用途：每个 Phase 结束后执行，确认无回归

- [x] **P1-T7**：新增 `/api/query/history` 接口（只读）
  - 文件：`doris-api/main.py`
  - 返回字段：`id`、`question`、`sql`、`table_names`、`is_empty_result`、`row_count`、`created_at`

### P1 验证命令

```bash
# 1. 确认表存在
docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot \
  -e "SHOW TABLES IN doris_db LIKE '_sys_query_history';"

# 2. 执行一次自然语言查询（需携带 X-API-Key）
curl -X POST http://localhost:38018/api/query/natural \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${SMATRIX_API_KEY}" \
  -d '{"query": "广州有多少机构？"}'

# 3. 查看历史写入
curl -H "X-API-Key: ${SMATRIX_API_KEY}" \
  http://localhost:38018/api/query/history

# 4. 再次查询，观察 API 日志中 [RAG] retrieved N examples（N>0）
# 5. 运行黄金测试集
python doris-api/tests/run_golden.py
```

---

## Phase 2：Meta-Agent 升级

**预计工期**：3-4 天
**价值**：新表上传后自动配置，无需手动维护字段规则

### P2 任务清单

- [x] **P2-T1**：创建 `_sys_table_agents` 和 `_sys_field_catalog` 表
  - 均使用 UNIQUE KEY（非 DUPLICATE KEY）
  - `_sys_table_agents` 含 `source_hash` 字段

- [x] **P2-T2**：明确 `_sys_table_metadata` vs `_sys_table_agents` 分工
  - `_sys_table_metadata`（保留）：`metadata_analyzer.py` 输出的原始 LLM 分析文本
  - `_sys_table_agents`（新增）：从 metadata 派生的结构化操作配置，含字段语义类型、匹配策略、枚举值
  - 转换步骤：读 `_sys_table_metadata` → LLM 解析为结构化 JSON → 写 `_sys_table_agents`
  - 冗余检测：`source_hash` 未变则跳过重新生成

- [x] **P2-T3**：上传接口触发 Meta-Agent
  - 文件：`doris-api/main.py` 的 `/api/upload`
  - 上传成功后异步调用：分析 → 写 `_sys_table_agents` + `_sys_field_catalog`
  - 新增 `/api/agents/{table_name}` 接口（Phase 2 验证用）

- [x] **P2-T4**：`generate_sql()` 注入 agent_config
  - 从 `_sys_table_agents` 读取匹配表的 agent_config，注入字段枚举值和 CoT 模板

- [x] **P2-T5**：定期刷新 `_sys_field_catalog`
  - APScheduler 每天凌晨重新 DISTINCT 枚举值

- [x] **P2-T6**：新增 `/api/query/history/{id}/feedback` 接口
  - 允许将 `quality_gate` 从 1 改为 2（手动拒绝）
  - 这是 quality_gate 状态机唯一的状态迁移入口（P1 只写 1，P2 允许降为 2）

### P2 验证命令

```bash
# 上传 Excel 后查看 Agent 配置（期望包含字段语义和枚举值）
curl -H "X-API-Key: ${SMATRIX_API_KEY}" \
  http://localhost:38018/api/agents/institutions

# 测试反馈接口（将某条历史标记为拒绝）
curl -X POST -H "X-API-Key: ${SMATRIX_API_KEY}" \
  http://localhost:38018/api/query/history/1/feedback \
  -d '{"quality_gate": 2}'
```

---

## Phase 3：多 Agent 编排

**预计工期**：5-7 天
**价值**：支持复杂多维查询 + 跨表查询

### P3 任务清单

- [x] **P3-T0**（前置）：构建 `_sys_table_relationships` 和关系发现机制
  - Schema 见上方建表 SQL
  - LLM 分析字段名语义 → 推断候选 JOIN key，写入 `confidence < 1.0`
  - 人工覆盖：POST `/api/relationships`，写入 `is_manual=True, confidence=1.0`
  - Coordinator 优先使用 `is_manual=True` 的记录

- [x] **P3-T1**：实现 Planner Agent
  - 新文件：`doris-api/planner_agent.py`
  - 输入：用户问题 + 可用表列表
  - 输出：`{"intent": "...", "tables": [...], "subtasks": [...], "needs_join": bool}`

- [x] **P3-T2**：实现 Table Admin Agent
  - 新文件：`doris-api/table_admin_agent.py`
  - 输入：子任务 + 该表 agent_config + 字段枚举值 + RAG 历史
  - 输出：SQL

- [x] **P3-T3**：实现 Coordinator Agent
  - 新文件：`doris-api/coordinator_agent.py`
  - 单表：透传；多表：优先 JOIN SQL，回退应用层聚合

- [x] **P3-T4**：重构 `/api/query/natural`
  - 替换当前直接调用 Vanna 的逻辑
  - 改为：Planner → Table Admins → Coordinator → Execute

- [x] **P3-T5**（可选）：引入 LangGraph
  - 当手写状态机复杂度 > 200 行时评估迁移
  - 决策标准见 `decisions/003-no-langgraph-until-phase3.md`

### P3 验证命令

```bash
# 测试跨表查询（需要先有 2+ 张表）
curl -X POST http://localhost:38018/api/query/natural \
  -H "X-API-Key: ${SMATRIX_API_KEY}" \
  -d '{"query": "2022年广州收入超5万的机构中，哪些参加了2023年的活动？"}'
```

---

## Phase 4：Auto-Repair + 生产加固

**预计工期**：3-4 天

### P4 任务清单

- [x] **P4-T1**：Auto-Repair Agent
  - 新文件：`doris-api/repair_agent.py`
  - 触发：SQL 执行报错，最多重试 2 次
  - 输入：原始问题 + 失败 SQL + 错误信息 + DDL
  - 成功后写入 `_sys_query_history`

- [x] **P4-T2**：连接池
  - 使用 `DBUtils` 或 `sqlalchemy` 连接池，最大连接数 10

- [x] **P4-T3**：DDL + 枚举值 TTL 缓存
  - `cachetools.TTLCache`：DDL 缓存 5 分钟，枚举值缓存 30 分钟

- [x] **P4-T4**：完整认证升级
  - 将 SEC-2 的 `X-API-Key` 升级为 Bearer Token / OAuth
  - 完善 CORS 白名单（生产域名）

- [x] **P4-T5**：MCP Server 包装
  - 新文件：`doris-api/mcp_server.py`
  - 依赖：`mcp`（官方 Python SDK，`pip install mcp`）
  - 暴露以下工具：
    - `query_natural(question: str) -> dict`：自然语言查询，返回 SQL + 结果
    - `list_tables() -> list`：列出所有可查询的业务表及描述
    - `get_table_schema(table_name: str) -> dict`：获取指定表的字段结构
    - `upload_excel(file_path: str, description: str) -> dict`：上传 Excel 并建表（可选）
  - 启动方式：`python doris-api/mcp_server.py`（stdio 传输，适配 Claude Code / Cursor 等）
  - 同时写 `mcp_config.json`（标准 MCP 配置，方便用户一行添加到 Claude Code settings）

    ```json
    {
      "mcpServers": {
        "smatrix": {
          "command": "python3",
          "args": ["doris-api/mcp_server.py"],
          "env": {
            "SMATRIX_API_URL": "http://localhost:38018",
            "SMATRIX_API_KEY": "${SMATRIX_API_KEY}"
          }
        }
      }
    }
    ```

  - MCP Server 本身**不直连 Doris**，只调用现有 REST API（保持单一入口，复用认证逻辑）

### P4 验证命令

```bash
# 测试无 Key 请求应返回 401
curl http://localhost:38018/api/query/natural

# 测试 Auto-Repair（构造语法错误的查询触发修复）

# 测试 MCP Server（列出工具清单）
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | python doris-api/mcp_server.py

# 测试 MCP query_natural 工具
echo '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query_natural","arguments":{"question":"广州有多少机构？"}}}' \
  | python doris-api/mcp_server.py
```

---

## Phase 5：Doris 向量索引

**预计工期**：3-5 天
**前提**：Phase 1-3 稳定运行，`_sys_query_history` 积累 ≥ 500 条有效记录

### P5 任务清单

- [x] **P5-T1**：选择中文 embedding 模型
  - 推荐：`BAAI/bge-small-zh-v1.5`（512 维，~100MB）
  - 备选：`BAAI/bge-m3`（1024 维，多语言，~600MB）

- [x] **P5-T2**：在 Doris 中追加向量列

  ```sql
  ALTER TABLE `_sys_query_history`
  ADD COLUMN `question_embedding` ARRAY<FLOAT>;
  -- 然后添加 HNSW 向量索引
  ```

- [x] **P5-T3**：实现 embedding 生成模块
  - 新文件：`doris-api/embedding.py`
  - 写入历史时同步生成 embedding

- [x] **P5-T4**：将 `get_similar_question_sql()` 改为向量检索
  - 替换 Doris 全文检索，用向量相似度查询返回 Top-K；ANN 索引创建为 best-effort

### P5 验证命令

```bash
# 测试语义检索（同义问法）
# 历史入库："广州的机构数量"
# 查询："在广州的组织有几家"
# 期望：检索到历史记录（语义相似但关键词不同）
```

---

## 全局验证标准

所有 Phase 完成后，以下测试必须全部通过：

```bash
# 1. 简单单表查询
"广州有多少机构？" → SQL 正确，执行无报错

# 2. 多维度组合
"2022年广东省收入超5万的机构有多少？" → WHERE 包含年份 + 省份 + 收入

# 3. 模糊匹配
"来自广州的机构" → WHERE 所在市 LIKE '%广州%'（不是 = '广州'）

# 4. 自学习验证
相同问题第二次查询 → API 日志出现 [RAG] retrieved N examples (N>0)

# 5. 跨表查询（Phase 3 后）
涉及 2 张表的问题 → Coordinator 生成 JOIN SQL 或应用层聚合

# 6. Auto-Repair（Phase 4 后）
构造会报错的查询 → 系统自动修复并返回结果

# 7. 黄金测试集（持续）
python doris-api/tests/run_golden.py → 全部通过
```
