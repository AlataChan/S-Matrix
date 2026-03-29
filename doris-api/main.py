"""
Doris API Gateway - 主程序
"""
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import uvicorn
import traceback
import os
import asyncio
import logging
import inspect

from config import API_HOST, API_PORT, DORIS_CONFIG
from handlers import action_handler
from db import doris_client
from upload_handler import excel_handler
from vanna_doris import VannaDorisOpenAI
from datasource_handler import datasource_handler, sync_scheduler
from metadata_analyzer import metadata_analyzer
from planner_agent import PlannerAgent
from table_admin_agent import TableAdminAgent
from coordinator_agent import CoordinatorAgent
from repair_agent import RepairAgent

app = FastAPI(
    title="Doris API Gateway",
    description="极简的 HTTP API Gateway for Apache Doris",
    version="1.0.0"
)


# Global readiness flag for Doris init to avoid 502s after reboot.
doris_ready = False
logger = logging.getLogger(__name__)
cors_origins = [
    origin.strip()
    for origin in os.getenv("SMATRIX_CORS_ORIGINS", "http://localhost:35173").split(",")
    if origin.strip()
]

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ 启动事件 ============

def _init_doris_sync():
    import time
    import pymysql

    retry_interval = int(os.getenv("DORIS_INIT_RETRY_INTERVAL", "2"))
    db_name = DORIS_CONFIG["database"]

    print("=" * 60)
    print("Doris API Gateway starting...")
    print("=" * 60)

    while True:
        try:
            print("Waiting for Doris FE...")

            conn = pymysql.connect(
                host=DORIS_CONFIG['host'],
                port=DORIS_CONFIG['port'],
                user=DORIS_CONFIG['user'],
                password=DORIS_CONFIG['password'],
                connect_timeout=5
            )

            cursor = conn.cursor()

            cursor.execute("SHOW BACKENDS")
            backends = cursor.fetchall()
            if not backends:
                be_host = os.getenv('DORIS_STREAM_LOAD_HOST', 'doris-be')
                be_heartbeat_port = 9050
                print(f"No BE registered, trying to add: {be_host}:{be_heartbeat_port}")
                try:
                    cursor.execute(f'ALTER SYSTEM ADD BACKEND "{be_host}:{be_heartbeat_port}"')
                    print(f"Sent add BE: {be_host}:{be_heartbeat_port}")
                    time.sleep(5)
                except Exception as be_err:
                    print(f"Add BE failed (may already exist): {be_err}")

            print(f"Ensure database {db_name}")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]

            if db_name in databases:
                print(f"Database '{db_name}' ready")
            else:
                print(f"Database '{db_name}' create failed")

            cursor.close()
            conn.close()

            datasource_handler.init_tables()
            print("System tables initialized")

            print("=" * 60)
            print("Doris API Gateway ready")
            print(f"Database: {db_name}")
            print(f"API: http://{API_HOST}:{API_PORT}")
            print(f"Docs: http://{API_HOST}:{API_PORT}/docs")
            print("=" * 60)
            return True

        except Exception as e:
            print(f"Connect failed: {e}")
            print(f"Retry in {retry_interval}s...")
            time.sleep(retry_interval)


@app.on_event("startup")
async def startup_event():
    """
    Application startup initialization.
    """
    async def init_in_background():
        global doris_ready
        if doris_ready:
            return
        ready = await asyncio.to_thread(_init_doris_sync)
        if ready:
            doris_ready = True
            sync_scheduler.start()

    asyncio.create_task(init_in_background())


@app.middleware("http")
async def api_guard_middleware(request: Request, call_next):
    if request.url.path.startswith("/api") and request.url.path != "/api/health":
        expected_api_key = os.getenv("SMATRIX_API_KEY")
        if not expected_api_key:
            return JSONResponse(
                status_code=503,
                content={"success": False, "message": "SMATRIX_API_KEY is not configured."},
            )

        provided_api_key = request.headers.get("X-API-Key")
        authorization = request.headers.get("Authorization", "")
        if authorization.lower().startswith("bearer "):
            provided_api_key = authorization.split(" ", 1)[1].strip()

        if provided_api_key != expected_api_key:
            return JSONResponse(
                status_code=401,
                content={"success": False, "message": "Unauthorized"},
            )

        if not doris_ready:
            return JSONResponse(
                status_code=503,
                content={
                    "success": False,
                    "message": "Doris FE is not ready yet, please retry later."
                },
            )
    return await call_next(request)


# ============ 数据模型 ============

class ExecuteRequest(BaseModel):
    """统一执行请求"""
    action: str = Field(..., description="操作类型: query/sentiment/classify/extract/stats/similarity/translate/summarize/mask/fixgrammar/generate/filter")
    table: Optional[str] = Field(None, description="表名")
    column: Optional[str] = Field(None, description="列名")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="其他参数")
    
    class Config:
        json_schema_extra = {
            "example": {
                "action": "sentiment",
                "table": "customer_feedback",
                "column": "feedback_text",
                "params": {
                    "limit": 50
                }
            }
        }


class LLMConfigRequest(BaseModel):
    """LLM 配置请求"""
    resource_name: str = Field(..., description="资源名称")
    provider_type: str = Field(..., description="厂商类型: openai/deepseek/qwen/zhipu/local等")
    endpoint: str = Field(..., description="API 端点")
    model_name: str = Field(..., description="模型名称")
    api_key: Optional[str] = Field(None, description="API 密钥")
    temperature: Optional[float] = Field(None, description="温度参数 0-1")
    max_tokens: Optional[int] = Field(None, description="最大 token 数")

    class Config:
        json_schema_extra = {
            "example": {
                "resource_name": "my_openai",
                "provider_type": "openai",
                "endpoint": "https://api.openai.com/v1/chat/completions",
                "model_name": "gpt-4",
                "api_key": "sk-xxxxx"
            }
        }


class NLQueryRequest(BaseModel):
    """自然语言查询请求"""
    question: str = Field(..., description="自然语言问题")
    table_name: str = Field(..., description="目标表名")
    resource_name: Optional[str] = Field(None, description="LLM 资源名称,不指定则使用第一个可用资源")

    class Config:
        json_schema_extra = {
            "example": {
                "question": "2022年的机构中来自于广东的有多少个?分别是来自于广东那几个城市每个城市的占比是多少?",
                "table_name": "中国环保公益组织现状调研数据2022.",
                "resource_name": "my_deepseek"
            }
        }


# ============ API 路由 ============

@app.get("/")
async def root():
    """健康检查"""
    return {
        "service": "Doris API Gateway",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/api/health")
async def health_check():
    """检查 Doris 连接状态"""
    try:
        result = doris_client.execute_query("SELECT 1 AS health")
        return {
            "success": True,
            "doris_connected": True,
            "message": "Doris connection OK"
        }
    except Exception as e:
        return {
            "success": False,
            "doris_connected": False,
            "error": str(e)
        }


@app.post("/api/execute")
async def execute_action(req: ExecuteRequest):
    """
    统一执行接口
    
    支持的 action:
    - query: 普通查询
    - sentiment: 情感分析
    - classify: 文本分类
    - extract: 信息提取
    - stats: 统计分析
    - similarity: 语义相似度
    - translate: 文本翻译
    - summarize: 文本摘要
    - mask: 敏感信息脱敏
    - fixgrammar: 语法纠错
    - generate: 内容生成
    - filter: 布尔过滤
    """
    try:
        # 合并参数
        params = req.params or {}
        if req.table:
            params['table'] = req.table
        if req.column:
            params['column'] = req.column
        
        # 执行操作
        result = await action_handler.execute_async(req.action, params)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


@app.get("/api/tables")
async def list_tables():
    """获取所有表"""
    try:
        tables = doris_client.get_tables()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    """获取表结构"""
    try:
        schema = doris_client.get_table_schema(table_name)
        return {
            "success": True,
            "table": table_name,
            "schema": schema
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/config")
async def create_llm_config(req: LLMConfigRequest):
    """创建 LLM 配置"""
    try:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"=== Received request: provider={req.provider_type}, endpoint={req.endpoint}, model={req.model_name}")

        # 构造 CREATE RESOURCE SQL (Doris 4.0 使用 'ai' 类型和 'ai.' 前缀)
        properties = [
            "'type' = 'ai'",
            f"'ai.provider_type' = '{req.provider_type}'",
            f"'ai.endpoint' = '{req.endpoint}'",
            f"'ai.model_name' = '{req.model_name}'"
        ]

        if req.api_key:
            properties.append(f"'ai.api_key' = '{req.api_key}'")
        if req.temperature is not None:
            properties.append(f"'ai.temperature' = {req.temperature}")
        if req.max_tokens is not None:
            properties.append(f"'ai.max_tokens' = {req.max_tokens}")
        
        properties_str = ',\n    '.join(properties)
        
        sql = f"""
        CREATE RESOURCE '{req.resource_name}'
        PROPERTIES (
            {properties_str}
        )
        """

        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"=== Creating LLM Resource SQL: {sql}")

        doris_client.execute_update(sql)
        
        return {
            "success": True,
            "message": f"LLM resource '{req.resource_name}' created successfully",
            "sql": sql
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


@app.get("/api/llm/config")
async def list_llm_configs():
    """获取所有 LLM 配置"""
    try:
        # Doris 4.0 的 SHOW RESOURCES 语法,使用 NAME LIKE 获取所有资源
        sql = 'SHOW RESOURCES WHERE NAME LIKE "%"'
        all_resources = doris_client.execute_query(sql)

        # SHOW RESOURCES 返回的是每个资源的每个属性作为一行
        # 需要按资源名称分组,并过滤出 AI 类型的资源
        resources_dict = {}
        for row in all_resources:
            name = row.get('Name')
            resource_type = row.get('ResourceType')

            # 只处理 AI 类型的资源
            if resource_type != 'ai':
                continue

            # 初始化资源对象 (使用前端期望的字段名)
            if name not in resources_dict:
                resources_dict[name] = {
                    'ResourceName': name,
                    'ResourceType': resource_type,
                    'properties': {}
                }

            # 收集属性
            item = row.get('Item')
            value = row.get('Value')
            if item and value:
                resources_dict[name]['properties'][item] = value

        # 转换为列表
        llm_resources = list(resources_dict.values())

        return {
            "success": True,
            "resources": llm_resources,
            "count": len(llm_resources)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/config/{resource_name}/test")
async def test_llm_config(resource_name: str):
    """测试 LLM 配置"""
    try:
        # 使用简单的测试查询 (Doris 4.0 使用 AI_GENERATE 函数)
        sql = f"SELECT AI_GENERATE('{resource_name}', 'Hello') AS test_result"
        result = doris_client.execute_query(sql)
        
        return {
            "success": True,
            "message": "LLM resource is working",
            "test_result": result[0] if result else None
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(e)
            }
        )


@app.delete("/api/llm/config/{resource_name}")
async def delete_llm_config(resource_name: str):
    """删除 LLM 配置"""
    try:
        sql = f"DROP RESOURCE '{resource_name}'"
        doris_client.execute_update(sql)

        return {
            "success": True,
            "message": f"LLM resource '{resource_name}' deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query/natural")
async def natural_language_query(request: Dict[str, Any]):
    """
    自然语言查询接口 (Agent-to-Agent) - 使用 Vanna.AI

    前端 Agent 传入自然语言问题,系统使用 Vanna.AI 生成 SQL 并执行查询

    Request Body:
        {
            "query": "2022年的机构中来自于广东的有多少个?分别是来自于广东那几个城市每个城市的占比是多少?",
            "api_key": "sk-xxx",  // 可选,默认从环境变量读取
            "model": "deepseek-chat",  // 可选,默认 deepseek-chat
            "base_url": "https://api.deepseek.com"  // 可选,默认 DeepSeek API
        }

    Response:
        {
            "success": true,
            "query": "原始问题",
            "sql": "生成的 SQL",
            "data": [...],
            "count": 数据行数
        }
    """
    try:
        query = request.get('query')
        if not query:
            raise HTTPException(status_code=400, detail="Missing 'query' parameter")

        # 获取 API 配置
        api_key = request.get('api_key') or os.getenv('DEEPSEEK_API_KEY') or os.getenv('OPENAI_API_KEY')
        model = request.get('model') or os.getenv('DEEPSEEK_MODEL', 'deepseek-chat')
        base_url = request.get('base_url') or os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')

        if not api_key:
            raise HTTPException(
                status_code=400,
                detail="API key not provided. Please provide 'api_key' in request or set DEEPSEEK_API_KEY/OPENAI_API_KEY environment variable"
            )

        logger.info(f"=== Natural language query: {query}")
        logger.info(f"=== Using model: {model} at {base_url}")

        api_config = {"api_key": api_key, "model": model, "base_url": base_url}
        try:
            tables_context = await datasource_handler.list_table_registry()
        except Exception as registry_error:
            logger.warning("failed to load table registry for planner, fallback to empty context: %s", registry_error)
            tables_context = []

        planner = PlannerAgent(tables_context=tables_context)
        plan = await asyncio.to_thread(planner.plan, query)
        subtasks = plan.get("subtasks") or [{"table": table, "question": query} for table in plan.get("tables", [])]
        table_admin = TableAdminAgent(doris_client_override=doris_client)

        sql_map: Dict[str, str] = {}
        for subtask in subtasks:
            table_name = subtask.get("table")
            if not table_name:
                continue
            sql_map[table_name] = await asyncio.to_thread(
                table_admin.generate_sql_for_subtask,
                subtask,
                query,
                api_config,
            )

        if not sql_map:
            raise HTTPException(status_code=400, detail="Planner could not resolve any target tables")

        try:
            relationships = await datasource_handler.list_relationships_async(plan.get("tables"))
        except Exception as relationship_error:
            logger.warning("failed to load relationships, fallback to none: %s", relationship_error)
            relationships = []

        coordinator = CoordinatorAgent()
        coordinate_signature = inspect.signature(coordinator.coordinate)
        if "relationships" in coordinate_signature.parameters:
            generated_sql = await asyncio.to_thread(
                coordinator.coordinate,
                plan,
                sql_map,
                relationships=relationships,
            )
        else:
            generated_sql = await asyncio.to_thread(coordinator.coordinate, plan, sql_map)

        history_vanna = VannaDorisOpenAI(
            doris_client=doris_client,
            api_key=api_key,
            model=model,
            base_url=base_url,
            config={'temperature': 0.1}
        )
        repair_agent = RepairAgent(
            doris_client=doris_client,
            api_key=api_key,
            model=model,
            base_url=base_url,
        )

        logger.info(f"=== Generated SQL: {generated_sql}")

        final_sql = generated_sql
        try:
            query_result = await doris_client.execute_query_async(final_sql)
        except Exception as execute_error:
            logger.warning("=== SQL execution failed, starting repair flow: %s", execute_error)
            if hasattr(history_vanna, "get_related_ddl"):
                ddl_list = await asyncio.to_thread(history_vanna.get_related_ddl, query)
            else:
                ddl_list = []
            last_error = execute_error

            for _ in range(2):
                repaired_sql = await asyncio.to_thread(
                    repair_agent.repair_sql,
                    query,
                    final_sql,
                    str(last_error),
                    ddl_list,
                    api_config=api_config,
                )
                final_sql = repaired_sql.strip().rstrip(";")
                logger.info("=== Repaired SQL: %s", final_sql)
                try:
                    query_result = await doris_client.execute_query_async(final_sql)
                    break
                except Exception as retry_error:
                    last_error = retry_error
            else:
                raise last_error

        try:
            await asyncio.to_thread(
                history_vanna.add_question_sql,
                question=query,
                sql=final_sql,
                row_count=len(query_result),
                is_empty_result=len(query_result) == 0,
            )
        except Exception as history_error:
            logger.warning("history persistence failed: %s", history_error)

        logger.info(f"=== Query executed successfully, returned {len(query_result)} rows")

        return {
            "success": True,
            "query": query,
            "sql": final_sql,
            "data": query_result,
            "count": len(query_result)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"=== Error in natural language query: {str(e)}")
        logger.error(traceback.format_exc())

        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


@app.get("/api/query/history")
async def query_history(limit: int = 100):
    """查询历史列表（只读）"""
    try:
        history = await datasource_handler.list_query_history_async(limit=limit)
        return {
            "success": True,
            "history": history,
            "count": len(history),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryHistoryFeedbackRequest(BaseModel):
    quality_gate: int = Field(..., description="质量门状态")


class RelationshipRequest(BaseModel):
    table_a: str
    column_a: str
    table_b: str
    column_b: str
    rel_type: Optional[str] = Field(default="logical")


@app.post("/api/query/history/{query_id}/feedback")
async def query_history_feedback(query_id: str, req: QueryHistoryFeedbackRequest):
    """更新查询历史质量标记"""
    try:
        return await datasource_handler.update_query_feedback_async(query_id, req.quality_gate)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/relationships")
async def create_relationship(req: RelationshipRequest):
    """创建手工关系覆盖"""
    try:
        return await datasource_handler.create_relationship_async(
            table_a=req.table_a,
            column_a=req.column_a,
            table_b=req.table_b,
            column_b=req.column_b,
            rel_type=req.rel_type or "logical",
            confidence=1.0,
            is_manual=True,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload/preview")
async def preview_excel_file(file: UploadFile = File(...), rows: int = 10):
    """预览 Excel 文件"""
    try:
        content = await file.read()
        result = await excel_handler.preview_excel_async(content, rows)

        return {
            "success": True,
            "filename": file.filename,
            **result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


async def _analyze_table_async(table_name: str, source_type: str):
    """异步分析表格元数据"""
    import asyncio
    await asyncio.sleep(2)  # 等待数据完全写入
    try:
        result = await metadata_analyzer.analyze_table_async(table_name, source_type)
        if result.get('success'):
            await asyncio.to_thread(metadata_analyzer.refresh_agent_assets, table_name, source_type)
            print(f"✅ 表格 '{table_name}' 元数据分析完成")
        else:
            print(f"⚠️ 表格 '{table_name}' 元数据分析失败: {result.get('error')}")
    except Exception as e:
        print(f"❌ 元数据分析异常: {e}")


@app.post("/api/upload")
async def upload_excel(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    column_mapping: Optional[str] = Form(None),
    create_table: str = Form("true")
):
    """
    上传 Excel 文件并导入到 Doris

    Args:
        file: Excel 文件
        table_name: 目标表名
        column_mapping: 列映射 JSON 字符串 (可选)
        create_table: 如果表不存在是否创建 (字符串 "true"/"false")
    """
    try:
        import json

        content = await file.read()

        # 解析列映射
        mapping = None
        if column_mapping:
            mapping = json.loads(column_mapping)

        # 转换 create_table 字符串为布尔值
        create_table_bool = create_table.lower() in ('true', '1', 'yes')

        result = await excel_handler.import_excel_async(
            file_content=content,
            table_name=table_name,
            column_mapping=mapping,
            create_table_if_not_exists=create_table_bool
        )
        if result.get('success'):
            await datasource_handler.ensure_table_registry_async(table_name, 'excel')

        # 自动触发元数据分析（异步，不阻塞返回）
        try:
            import asyncio
            if result.get('success'):
                asyncio.create_task(_analyze_table_async(table_name, 'excel'))
        except Exception as analyze_error:
            print(f"⚠️ 元数据分析触发失败: {analyze_error}")

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": traceback.format_exc()
            }
        )


# ============ 数据源同步 API ============

class DataSourceTestRequest(BaseModel):
    """数据源连接测试请求"""
    host: str = Field(..., description="数据库主机")
    port: int = Field(..., description="数据库端口")
    user: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: Optional[str] = Field(None, description="数据库名")


class DataSourceSaveRequest(BaseModel):
    """保存数据源请求"""
    name: str = Field(..., description="数据源名称")
    host: str = Field(..., description="数据库主机")
    port: int = Field(..., description="数据库端口")
    user: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")


class SyncTableRequest(BaseModel):
    """同步表请求"""
    source_table: str = Field(..., description="源表名")
    target_table: Optional[str] = Field(None, description="目标表名")


class SyncMultipleRequest(BaseModel):
    """批量同步请求"""
    tables: List[Dict[str, str]] = Field(..., description="要同步的表列表")


@app.post("/api/datasource/test")
async def test_datasource_connection(req: DataSourceTestRequest):
    """测试数据源连接"""
    result = await datasource_handler.test_connection(
        host=req.host,
        port=req.port,
        user=req.user,
        password=req.password,
        database=req.database
    )
    return result


@app.post("/api/datasource")
async def save_datasource(req: DataSourceSaveRequest):
    """保存数据源配置"""
    try:
        result = await datasource_handler.save_datasource(
            name=req.name,
            host=req.host,
            port=req.port,
            user=req.user,
            password=req.password,
            database=req.database
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/datasource")
async def list_datasources():
    """获取所有数据源"""
    try:
        datasources = await datasource_handler.list_datasources()
        return {
            "success": True,
            "datasources": datasources,
            "count": len(datasources)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/datasource/{ds_id}")
async def delete_datasource(ds_id: str):
    """删除数据源"""
    try:
        result = await datasource_handler.delete_datasource(ds_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/datasource/{ds_id}/tables")
async def get_datasource_tables(ds_id: str):
    """获取数据源中的表列表"""
    try:
        print(f"📋 获取数据源表列表: ds_id={ds_id}")
        ds = await datasource_handler.get_datasource(ds_id)
        print(f"📋 数据源信息: {ds}")
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")

        result = await datasource_handler.get_remote_tables(
            host=ds['host'],
            port=ds['port'],
            user=ds['user'],
            password=ds['password'],
            database=ds['database_name']
        )
        print(f"📋 获取表列表结果: {result}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"❌ 获取表列表异常: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/datasource/{ds_id}/sync")
async def sync_datasource_table(ds_id: str, req: SyncTableRequest):
    """同步单个表"""
    try:
        result = await datasource_handler.sync_table(
            ds_id=ds_id,
            source_table=req.source_table,
            target_table=req.target_table
        )
        if not result.get('success'):
            raise HTTPException(status_code=500, detail=result.get('error'))

        # 自动触发元数据分析
        target = req.target_table or req.source_table
        try:
            await datasource_handler.ensure_table_registry_async(target, 'database_sync')
            import asyncio
            asyncio.create_task(_analyze_table_async(target, 'database_sync'))
        except Exception as analyze_error:
            print(f"⚠️ 元数据分析触发失败: {analyze_error}")

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/datasource/{ds_id}/sync-multiple")
async def sync_multiple_tables(ds_id: str, req: SyncMultipleRequest):
    """批量同步多个表"""
    try:
        result = await datasource_handler.sync_multiple_tables(
            ds_id=ds_id,
            tables=req.tables
        )

        # 为每个成功同步的表触发元数据分析
        if result.get('results'):
            import asyncio
            for table_result in result['results']:
                if table_result.get('success'):
                    target = table_result.get('target_table')
                    try:
                        await datasource_handler.ensure_table_registry_async(target, 'database_sync')
                        asyncio.create_task(_analyze_table_async(target, 'database_sync'))
                    except Exception as e:
                        print(f"⚠️ 元数据分析触发失败: {e}")

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 表预览 API ============

@app.get("/api/datasource/{ds_id}/tables/{table_name}/preview")
async def preview_datasource_table(ds_id: str, table_name: str, limit: int = 100):
    """预览远程表的结构和数据"""
    try:
        ds = await datasource_handler.get_datasource(ds_id)
        if not ds:
            raise HTTPException(status_code=404, detail="数据源不存在")

        result = await datasource_handler.preview_remote_table(
            host=ds['host'],
            port=ds['port'],
            user=ds['user'],
            password=ds['password'],
            database=ds['database_name'],
            table_name=table_name,
            limit=limit
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 同步任务调度 API ============

class ScheduleSyncRequest(BaseModel):
    """定时同步请求（增强版）"""
    datasource_id: str = Field(..., description="数据源ID")
    source_table: str = Field(..., description="源表名")
    target_table: Optional[str] = Field(None, description="目标表名")
    schedule_type: str = Field(..., description="调度类型: hourly/daily/weekly/monthly")
    schedule_minute: Optional[int] = Field(0, description="分钟 (0-59)")
    schedule_hour: Optional[int] = Field(0, description="小时 (0-23)")
    schedule_day_of_week: Optional[int] = Field(1, description="周几 (1-7, 1=周一)")
    schedule_day_of_month: Optional[int] = Field(1, description="日期 (1-31)")
    enabled_for_ai: Optional[bool] = Field(True, description="是否启用AI分析")


class UpdateSyncTaskRequest(BaseModel):
    """更新同步任务请求"""
    schedule_type: Optional[str] = Field(None, description="调度类型")
    schedule_minute: Optional[int] = Field(None, description="分钟")
    schedule_hour: Optional[int] = Field(None, description="小时")
    schedule_day_of_week: Optional[int] = Field(None, description="周几")
    schedule_day_of_month: Optional[int] = Field(None, description="日期")
    enabled_for_ai: Optional[bool] = Field(None, description="是否启用AI分析")




class UpdateTableRegistryRequest(BaseModel):
    """????????????????????????"""
    display_name: Optional[str] = Field(None, description="????????????")
    description: Optional[str] = Field(None, description="?????????")

@app.post("/api/sync/schedule")
async def create_sync_schedule(req: ScheduleSyncRequest):
    """创建定时同步任务"""
    try:
        result = await datasource_handler.save_sync_task(
            ds_id=req.datasource_id,
            source_table=req.source_table,
            target_table=req.target_table,
            schedule_type=req.schedule_type,
            schedule_minute=req.schedule_minute or 0,
            schedule_hour=req.schedule_hour or 0,
            schedule_day_of_week=req.schedule_day_of_week or 1,
            schedule_day_of_month=req.schedule_day_of_month or 1,
            enabled_for_ai=req.enabled_for_ai if req.enabled_for_ai is not None else True
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/sync/tasks/{task_id}")
async def update_sync_task(task_id: str, req: UpdateSyncTaskRequest):
    """更新同步任务配置"""
    try:
        result = await datasource_handler.update_sync_task(
            task_id=task_id,
            schedule_type=req.schedule_type,
            schedule_minute=req.schedule_minute,
            schedule_hour=req.schedule_hour,
            schedule_day_of_week=req.schedule_day_of_week,
            schedule_day_of_month=req.schedule_day_of_month,
            enabled_for_ai=req.enabled_for_ai
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/sync/tasks/{task_id}/toggle-ai")
async def toggle_task_ai(task_id: str, enabled: bool):
    """切换同步任务的AI分析启用状态"""
    try:
        result = await datasource_handler.toggle_ai_enabled(task_id, enabled)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/tasks")
async def list_sync_tasks():
    """获取所有同步任务"""
    try:
        tasks = await datasource_handler.list_sync_tasks()
        return {
            "success": True,
            "tasks": tasks,
            "count": len(tasks)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sync/ai-enabled-tables")
async def get_ai_enabled_tables():
    """获取所有启用AI分析的表名"""
    try:
        tables = await datasource_handler.get_ai_enabled_tables()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/sync/tasks/{task_id}")
async def delete_sync_task(task_id: str):
    """删除同步任务"""
    try:
        result = await datasource_handler.delete_sync_task(task_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ 元数据分析 API ============

@app.post("/api/tables/{table_name}/analyze")
async def analyze_table_metadata(table_name: str, source_type: str = "manual"):
    """分析表格元数据"""
    try:
        result = await metadata_analyzer.analyze_table_async(table_name, source_type)
        if not result.get('success'):
            raise HTTPException(status_code=500, detail=result.get('error'))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables/{table_name}/metadata")
async def get_table_metadata(table_name: str):
    """获取表格元数据"""
    try:
        metadata = metadata_analyzer.get_metadata(table_name)
        if not metadata:
            return {
                "success": True,
                "metadata": None,
                "message": "表格尚未分析，请先调用分析接口"
            }
        return {
            "success": True,
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agents/{table_name}")
async def get_table_agent(table_name: str):
    """获取表 Agent 配置"""
    try:
        agent = metadata_analyzer.get_agent_config(table_name)
        if not agent:
            return {
                "success": True,
                "agent": None,
                "message": "表 Agent 配置尚未生成，请先执行分析。"
            }
        return {"success": True, "agent": agent}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metadata")
async def list_all_metadata():
    """获取所有表格元数据"""
    try:
        metadata_list = metadata_analyzer.list_all_metadata()
        return {
            "success": True,
            "metadata": metadata_list,
            "count": len(metadata_list)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




# ============ ??????????????????API ============

@app.get("/api/table-registry")
async def list_table_registry():
    """???????????????????????????"""
    try:
        tables = await datasource_handler.list_table_registry()
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/table-registry/{table_name}")
async def update_table_registry(table_name: str, req: UpdateTableRegistryRequest):
    """????????????????????????????????????"""
    try:
        result = await datasource_handler.update_table_registry(
            table_name=table_name,
            display_name=req.display_name,
            description=req.description
        )
        if not result.get('success'):
            raise HTTPException(status_code=400, detail=result.get('error'))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True
    )
