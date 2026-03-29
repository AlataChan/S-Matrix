# ADR-002：激活 Vanna 而非替换它

> 状态：已采纳
> 日期：2026-03-27
> 阶段：Phase 1

---

## 背景

Vanna.AI 0.7.9 在项目中的 RAG 核心功能（存储、检索、向量化）全部被 stub 掉，实际上只是一个 Prompt 组装骨架。

面临选择：
1. 完全替换 Vanna，自己实现 Text-to-SQL 流水线
2. 保留 Vanna 骨架，实现它 stub 掉的方法
3. 引入其他框架（LangChain、LlamaIndex）

## 决策

**保留 Vanna 骨架，实现其 stub 方法（激活方案）。**

## 理由

### Vanna 骨架是合理的

`get_related_ddl()` → `get_sql_prompt()` → `submit_prompt()` 这个三段式流程本身是正确的 Text-to-SQL 设计。已有的优化（`auto_fuzzy_match_locations`、keyword-scored DDL 检索）不需要重写。

### stub 方法可以直接实现

需要实现的方法签名已经定义好：
```python
def add_question_sql(self, question: str, sql: str) -> str
def get_similar_question_sql(self, question: str) -> List[Dict]
def generate_embedding(self, data: str) -> List[float]
```

实现这些方法比从零搭建新框架代价低得多。

### 避免框架迁移成本

替换为 LangChain/LlamaIndex 需要重写现有的所有集成逻辑，引入大量新依赖，且框架本身的学习成本和 breaking changes 风险较高。

## 约束

- Phase 3 引入多 Agent 编排时，如果 Vanna 骨架阻碍了 Planner/Coordinator 的设计，再评估是否需要解耦
- `VannaDorisOpenAI` 当前强依赖 `requests` 库同步调用，Phase 3 需要评估是否改为 `httpx` 异步调用
