# S-Matrix 数据中台

基于 Apache Doris 4.0 的智能数据中台，集成 Vanna.AI Text-to-SQL，支持自然语言查询数据库。

**架构**：多 Agent 查询引擎（Planner → TableAdmin → Coordinator → RepairAgent）

---

## 快速开始

### 前置要求

- Docker 20.10+
- Docker Compose 2.0+
- 至少 8GB 可用内存

### 启动

```bash
git clone https://github.com/sga-jerrylin/S-Matrix.git
cd S-Matrix

# 初始化 .env（自动生成 SMATRIX_API_KEY 和 ENCRYPTION_KEY）
bash scripts/setup_env.sh

# 填入 LLM API Key（脚本会提示哪些字段需要手动填写）
vim .env

docker-compose up -d
docker-compose logs -f
```

等待 2-3 分钟初始化完成。

### 访问地址（宿主机端口）

| 服务 | 地址 |
| ---- | ---- |
| 前端界面 | <http://localhost:35173> |
| API 文档 | <http://localhost:38018/docs> |
| Doris FE WebUI | <http://localhost:38030>（root / 空密码） |

---

## 系统架构

```text
前端 (Vue 3)          http://localhost:35173
      │
      ▼
API Gateway           http://localhost:38018
(FastAPI + Vanna.AI)
      │  PlannerAgent    → 多表路由 + 意图识别
      │  TableAdminAgent → 单表 SQL 生成
      │  CoordinatorAgent→ 多表 JOIN 合并
      │  RepairAgent     → SQL 失败自动修复
      ▼
Apache Doris 4.0
  FE (38030) ◄──► BE (38040)
      │
      ▼
LLM (DeepSeek / OpenAI-compatible)
```

---

## 功能

| 功能 | 说明 |
| ---- | ---- |
| Excel 上传 | 自动建表 + Stream Load 批量导入，支持 replace / append 模式 |
| 自然语言查询 | Planner 多表路由 → TableAdmin 生成 SQL → 执行 → 结果返回 |
| 多表 JOIN | CoordinatorAgent 根据表关系自动生成跨表 JOIN SQL |
| SQL 自动修复 | RepairAgent：执行失败时 LLM 自动修复，最多重试 2 次 |
| 查询记忆（RAG） | 成功 Q→SQL 自动存档，下次查询 Few-shot 注入 |
| 元数据分析 | 上传后 LLM 自动分析表结构和列语义，生成业务描述 |
| 模糊地名匹配 | 自动将地理字段精确匹配转为 LIKE，提升查询成功率 |
| LLM 配置管理 | 支持 OpenAI / DeepSeek / 通义等多提供商，存储于 Doris Resources |
| 数据源同步 | 从外部数据库同步表到 Doris（APScheduler 定时任务） |
| 统一执行接口 | query / sentiment / classify / extract 等 AI 操作 |
| API 认证 | `SMATRIX_API_KEY` 中间件，支持 `X-API-Key` 和 `Bearer` |
| MCP Server | stdio wrapper，支持外部 AI Agent 通过 MCP 协议接入 |
| Docker 部署 | 4 服务一键启动（Frontend + Backend + Doris FE/BE） |

---

## 配置说明

复制 `.env.example` 为 `.env` 后编辑：

```bash
# LLM 配置（必填其一）
DEEPSEEK_API_KEY=sk-your-key-here    # 从 platform.deepseek.com 获取
DEEPSEEK_MODEL=deepseek-chat
DEEPSEEK_BASE_URL=https://api.deepseek.com

# API 认证密钥（必填）
# 所有 /api/* 接口请求都需要在 Header 中携带此 Key
# X-API-Key: <your-key>  或  Authorization: Bearer <your-key>
SMATRIX_API_KEY=your-secret-api-key-here

# CORS 允许的前端地址（多个用逗号分隔）
SMATRIX_CORS_ORIGINS=http://localhost:35173
```

> `docker-compose.yml` 会自动读取 `.env` 中的变量，无需手动修改 compose 文件。

### 网络冲突处理

如遇 `Pool overlaps with other one on this address space`，修改 `docker-compose.yml` 中的网络段：

```yaml
networks:
  smatrix-network:
    ipam:
      config:
        - subnet: 192.168.200.0/24   # 改为未占用的网段
```

同步修改各服务的 `ipv4_address` 和 `FE_SERVERS` / `BE_ADDR` 环境变量。

---

## 常用命令

```bash
# 查看状态
docker-compose ps

# 查看日志
docker-compose logs -f smatrix-api

# 重启 API 服务
docker-compose restart smatrix-api

# 停止
docker-compose down

# 完全清理（含数据卷）
docker-compose down -v

# 重新构建
docker-compose up -d --build
```

---

## API 认证

所有 `/api/*` 接口（除 `/api/health`）需要提供认证 Key，二选一：

```bash
# 方式 1：X-API-Key Header
curl -H "X-API-Key: your-secret-api-key" http://localhost:38018/api/tables

# 方式 2：Bearer Token
curl -H "Authorization: Bearer your-secret-api-key" http://localhost:38018/api/tables
```

---

## 故障排查

### BE 节点不健康

```bash
docker logs smatrix-be

# 手动注册 BE
docker exec -it smatrix-fe mysql -h127.0.0.1 -P9030 -uroot \
  -e "ALTER SYSTEM ADD BACKEND '192.168.100.3:9050';"
```

### 数据库不存在

API 启动时会自动创建，手动创建：

```bash
docker exec -it smatrix-fe mysql -h127.0.0.1 -P9030 -uroot \
  -e "CREATE DATABASE IF NOT EXISTS doris_db;"
```

### API Key 失效（401 错误）

更新 `.env` 中的 `DEEPSEEK_API_KEY`，然后：

```bash
docker-compose up -d smatrix-api
```

### 启动后返回 503

Doris 初始化需要 2-3 分钟，503 表示 FE/BE 尚未就绪，稍等后重试即可。

---

## 技术栈

- **前端**：Vue 3.5 + TypeScript + Vite 7 + Ant Design Vue 4
- **后端**：Python 3.11 + FastAPI 0.115 + Uvicorn
- **数据库**：Apache Doris 4.0（1 FE + 1 BE）
- **AI**：Vanna.AI 0.7.9 + DeepSeek / OpenAI-compatible
- **部署**：Docker Compose

---

## 项目文档

| 文档 | 说明 |
| ---- | ---- |
| [API_EXAMPLES.md](./API_EXAMPLES.md) | HTTP API 调用示例（含认证） |
| [.plans/task_plan.md](.plans/task_plan.md) | 架构设计 + 任务清单 + 验证命令 |
| [.plans/findings.md](.plans/findings.md) | 代码库分析结论 + 踩坑记录 |
| [.plans/progress.md](.plans/progress.md) | 实现进度记录 |

---

## 联系方式

- GitHub: <https://github.com/sga-jerrylin/S-Matrix>
- Email: <jerrylin@sologenai.com>
