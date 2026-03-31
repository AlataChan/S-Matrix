# S-Matrix Progress

> 最后更新：2026-04-01（Expert 模式完成经营视图优化与混合式时间规划）
> 当前阶段：Completed

---

## 当前状态总览

| Phase | 名称 | 状态 | 完成度 |
| ----- | ---- | ---- | ------ |
| SEC | 最小安全加固（Phase 1 前置） | ✅ 完成 | 100% |
| Phase 0 | 基础设施 + 核心功能 | ✅ 完成 | 100% |
| Phase 1 | 激活 Vanna RAG | ✅ 完成 | 100% |
| Phase 2 | Meta-Agent 升级 | ✅ 完成 | 100% |
| Phase 3 | 多 Agent 编排 | ✅ 完成 | 100% |
| Phase 4 | Auto-Repair + 生产加固 | ✅ 完成 | 100% |
| Phase 5 | Doris 向量索引 | ✅ 完成 | 100% |

---

## 本轮完成项

### SEC

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| SEC-1 | ✅ | 根目录 `docker-compose.yml` 改为从环境变量读取 `DEEPSEEK_API_KEY` / `SMATRIX_API_KEY`；新增 `.env.example`；执行 `git rm --cached .env` |
| SEC-2 | ✅ | `main.py` 增加 API 认证中间件，支持 `X-API-Key` 和 `Authorization: Bearer` |
| SEC-3 | ✅ | CORS 白名单改为环境变量驱动，默认 `http://localhost:35173` |

### Phase 1

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| P1-T1 | ✅ | `datasource_handler.py` 新建 `_sys_query_history` 和全文索引 |
| P1-T2 | ✅ | `vanna_doris.py` 实现 `add_question_sql()`，采用 UUID、MD5 去重、空结果合法 |
| P1-T2b | ✅ | `extract_table_names()` 实现软提取和日志用途存储 |
| P1-T3 | ✅ | `get_similar_question_sql()` 支持向量优先、全文和 LIKE 回退 |
| P1-T4 | ✅ | `generate_sql()` 激活 RAG 注入并输出 `[RAG] retrieved N examples` 日志 |
| P1-T5 | ✅ | `/api/query/natural` 成功后同步写历史 |
| P1-T6 | ✅ | 新增 `doris-api/tests/golden_queries.json` 和 `doris-api/tests/run_golden.py` |
| P1-T7 | ✅ | 新增 `/api/query/history` 只读接口 |

### Phase 2

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| P2-T1 | ✅ | 新建 `_sys_table_agents`、`_sys_field_catalog` |
| P2-T2 | ✅ | `metadata_analyzer.py` 衍生结构化 agent 配置并做 `source_hash` 跳过 |
| P2-T3 | ✅ | 上传成功后异步触发 metadata + agent 资产刷新；新增 `/api/agents/{table_name}` |
| P2-T4 | ✅ | `generate_sql()` 注入 `agent_config` 和字段枚举信息 |
| P2-T5 | ✅ | APScheduler 增加 field catalog 定期刷新 |
| P2-T6 | ✅ | 新增 `/api/query/history/{id}/feedback` |

### Phase 3

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| P3-T0 | ✅ | 新建 `_sys_table_relationships`、关系写入接口 `/api/relationships` |
| P3-T1 | ✅ | 新增 `planner_agent.py` |
| P3-T2 | ✅ | 新增 `table_admin_agent.py` |
| P3-T3 | ✅ | 新增 `coordinator_agent.py` |
| P3-T4 | ✅ | `/api/query/natural` 改为 Planner → Table Admins → Coordinator → Execute |
| P3-T5 | ✅ | 本轮维持手写状态机，未引入 LangGraph |

### Phase 4

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| P4-T1 | ✅ | 新增 `repair_agent.py`，SQL 失败自动修复，最多重试 2 次 |
| P4-T2 | ✅ | `db.py` 引入 `DBUtils.PooledDB` 连接池，默认最大连接数 10 |
| P4-T3 | ✅ | `cachetools.TTLCache` 落地到 DDL 和枚举值缓存 |
| P4-T4 | ✅ | Bearer Token 认证已接入，兼容原 `X-API-Key` |
| P4-T5 | ✅ | 新增 `mcp_server.py` 和 `mcp_config.json`，通过 REST API 对外暴露工具 |

### Phase 5

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| P5-T1 | ✅ | 选型 `BAAI/bge-small-zh-v1.5`，实现可落地的 hashing fallback |
| P5-T2 | ✅ | 新增 `ensure_query_history_vector_support()`，尝试追加向量列和 ANN 索引 |
| P5-T3 | ✅ | 新增 `embedding.py`，历史写入时同步生成 embedding |
| P5-T4 | ✅ | `get_similar_question_sql()` 改为向量优先检索，文本检索回退 |

### Post-Phase Enhancements

| 任务 | 状态 | 落地内容 |
| ---- | ---- | -------- |
| EXP-1 | ✅ | Expert 报告主视图改为固定经营版式：`经营摘要 + 关键洞察 + 动作建议`，详细分析折叠保留证据链、根因、conversation、reasoning 与执行步骤 |
| EXP-2 | ✅ | Expert 分析改为混合式时间规划：后端先探测时间列跨度/稠密度/缺失率并给出 `day/week/month/quarter/year` 候选粒度，strategist 再选择 1-2 个高价值时间计划，executor 按允许时间列、粒度、窗口上限执行 |

---

## 测试与验收

### 分阶段验收

| Phase | 验收命令 | 结果 |
| ----- | -------- | ---- |
| Phase 1 | `pytest doris-api/tests/test_security_and_phase1.py -q` | ✅ 通过 |
| Phase 2 | `pytest doris-api/tests/test_phase2_agents.py -q` | ✅ 通过 |
| Phase 3 | `pytest doris-api/tests/test_phase3_orchestration.py -q` | ✅ 通过 |
| Phase 4 | `pytest doris-api/tests/test_phase4_production.py -q` | ✅ 通过 |
| Phase 5 | `pytest doris-api/tests/test_phase5_vector_search.py -q` | ✅ 通过 |

### 总回归 / 冒烟

| 检查项 | 命令 | 结果 |
| ---- | ---- | ---- |
| 后端测试总集 | `docker exec smatrix-api python -m pytest tests/ -q` | ✅ `51 passed, 4 skipped, 0 warnings` |
| Expert 分析链路回归 | `docker run --rm -v /Users/apple/S-Matrix/doris-api:/app -w /app s-matrix-smatrix-api python -m pytest tests/test_analyst_agent.py tests/test_analysis_api.py tests/test_analysis_scheduler.py tests/test_analysis_dispatcher.py tests/test_docker_startup_resilience.py -q` | ✅ `84 passed`（2026-04-01） |
| 前端构建 | `npm run build` | ✅ 通过 |
| Python 编译检查 | `python -m compileall doris-api` | ✅ 通过 |
| MCP Server 工具枚举 | `echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \| python doris-api/mcp_server.py` | ✅ 通过 |
| 黄金测试集 live API | `run_golden.py --base-url http://localhost:38018` | ✅ **5/5 通过**（2026-03-30 验收） |

---

## 风险与备注

- Phase 5 向量列 `question_embedding ARRAY<FLOAT>` 已落地，20 条记录含 512 维 embedding，向量检索功能正常。ANN（HNSW）索引因 Doris 4.0-rc02 限制（仅支持建表时创建，不支持 ALTER/CREATE INDEX 追加）未建成，当前为全表扫描匹配。数据量 <10K 条时性能无影响；后续若需 ANN 加速，需重建表或等待 Doris 后续版本支持。
- 弃用警告已全部清理（2026-03-30）：FastAPI `on_event` → `lifespan`；Pydantic `class Config` → `model_config = ConfigDict(...)`；`model_name` protected namespace 冲突 → `protected_namespaces=()`。

---

## 变更日志

| 日期 | 变更内容 | 相关文件 |
| ---- | -------- | -------- |
| 2026-03-28 | 完成 SEC + Phase 1-5 全部开发、测试和文档回写 | `.plans/*`、`doris-api/*`、`docker-compose.yml` |
| 2026-03-28 | 新增 MCP Server、embedding、repair agent、golden tests | `doris-api/mcp_server.py`、`doris-api/embedding.py`、`doris-api/repair_agent.py`、`doris-api/tests/*` |
| 2026-03-28 | 修复密钥管理、Bearer 认证、UUID 历史记录与关系表 | `docker-compose.yml`、`main.py`、`vanna_doris.py`、`datasource_handler.py` |
| 2026-03-30 | 集成验收：创建 golden_runner.py、黄金测试 5/5 通过、清理弃用警告（lifespan + ConfigDict + protected_namespaces）、确认向量索引状态 | `doris-api/tests/golden_runner.py`、`doris-api/tests/golden_queries.json`、`doris-api/main.py` |
| 2026-03-31 | Expert 报告改为经营友好版式，兼容历史报告的洞察/建议兜底渲染 | `doris-api/analyst_agent.py`、`doris-frontend/src/components/DataAnalysis.vue`、`doris-frontend/src/api/doris.ts` |
| 2026-04-01 | Expert 分析升级为混合式时间规划：时间维度探测、候选粒度选择、strategist time_plan、executor 时间边界与 fallback SQL | `doris-api/analyst_agent.py`、`doris-api/tests/test_analyst_agent.py` |
