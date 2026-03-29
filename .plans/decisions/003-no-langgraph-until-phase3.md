# ADR-003：Phase 3 之前不引入 LangGraph

> 状态：已采纳
> 日期：2026-03-27
> 阶段：Phase 1-2

---

## 背景

交接文档推荐使用 LangGraph（Supervisor + Worker 模式）实现多 Agent 编排。需要决定何时引入。

## 决策

**Phase 1 和 Phase 2 不引入 LangGraph。Phase 3 开始时评估是否引入。**

评估标准：如果手写状态机代码 > 200 行，则迁移到 LangGraph；否则继续用手写状态机。

## 理由

### Phase 1/2 根本不需要多 Agent

- Phase 1：只是给现有 Vanna 流水线增加存储和检索，单文件改动
- Phase 2：Meta-Agent 是单次 LLM 调用（分析表结构），无需编排框架

### 过早引入增加复杂度

LangGraph 引入以下成本：
- 新依赖：`langgraph`、`langchain-core`（依赖树较重）
- 概念学习：StateGraph、Node、Edge、Checkpoint 等概念
- 调试复杂性：Agent 状态转换在框架内部，不如手写状态机透明

### Phase 3 的多 Agent 逻辑本质上是线性流水线

```
Planner → [Table Admin 1, Table Admin 2, ...] → Coordinator
```

这个 DAG 结构用手写状态机 100-150 行就能实现。只有当逻辑变得动态（Agent 自主决定下一步、有循环、有条件分支）时，LangGraph 才有明显优势。

## 重新评估条件

在 Phase 3 开始时，如果出现以下情况则引入 LangGraph：
1. 需要动态决定 Agent 数量（不确定需要几个 Table Admin）
2. 需要 Agent 之间的反馈循环（Table Admin 结果影响 Planner 决策）
3. 需要检查点/断点恢复（长时间运行的查询）
