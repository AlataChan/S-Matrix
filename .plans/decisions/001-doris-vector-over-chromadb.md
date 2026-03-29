# ADR-001：用 Doris 向量索引替代 ChromaDB

> 状态：已采纳
> 日期：2026-03-27
> 阶段：Phase 5 实施

---

## 背景

Vanna.AI 的 RAG 需要一个向量存储后端来保存历史 Q→SQL 对的 embedding，并在查询时做相似度检索。

候选方案：
1. ChromaDB（Vanna 内置支持，底层 SQLite）
2. DuckDB（无向量支持，排除）
3. Doris 4.0 原生向量索引（HNSW）

## 决策

**采用 Doris 4.0 原生向量索引（Phase 5），Phase 1 过渡期用 Doris 全文检索。**

## 理由

### 反对 ChromaDB 的原因

1. **中文语义问题**：ChromaDB 默认 embedding 模型 `all-MiniLM-L6-v2` 为英文优化，中文语义向量质量差。需替换为 `BAAI/bge-small-zh-v1.5`，引入 `sentence-transformers` 依赖（~500MB 安装体积）。

2. **额外文件存储**：ChromaDB 持久化为本地文件，在容器环境中需要挂载卷，且只有 API 容器可访问，无法跨容器共享。

3. **运维成本**：引入额外的存储系统，需要备份、迁移策略，与 Doris 数据备份流程不一致。

### 选择 Doris 向量索引的原因

1. **零额外依赖**：Doris 4.0 已部署，原生支持 HNSW 向量索引，无需新增服务。

2. **统一存储**：业务数据和向量数据都在 Doris，统一备份、统一查询、统一运维。

3. **可与结构化查询组合**：可以在向量相似度检索基础上叠加过滤条件（如 `WHERE table_names = 'institutions'`），这是纯向量数据库做不到或较复杂的。

4. **中文模型灵活替换**：embedding 由 API 服务生成，模型选择不依赖 ChromaDB 的 embedding 函数接口，更灵活。

## 过渡方案

Phase 1 使用 Doris 全文检索（`MATCH_ANY` 或 LIKE 关键词匹配）作为 RAG 过渡：
- 中文短问句的关键词重叠率通常足够
- 完全零新增依赖
- Phase 5 切换为向量检索时，只需修改 `get_similar_question_sql()` 的实现

## 风险

- Doris 4.0 HNSW 向量索引的生产稳定性需要验证
- 需要确认当前部署的 Doris 4.0.0 镜像版本是否已包含向量索引功能
