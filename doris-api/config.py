"""
Doris API Gateway 配置文件
"""

import os

# Doris 连接配置
DORIS_CONFIG = {
    'host': os.getenv('DORIS_HOST', 'localhost'),
    'port': int(os.getenv('DORIS_PORT', '19030')),  # MySQL 协议端口
    'user': os.getenv('DORIS_USER', 'root'),
    'password': os.getenv('DORIS_PASSWORD', ''),
    'database': os.getenv('DORIS_DATABASE', 'doris_db'),
    'charset': 'utf8mb4'
}

# Doris Stream Load 配置
DORIS_STREAM_LOAD = {
    'host': os.getenv('DORIS_STREAM_LOAD_HOST', 'localhost'),
    'port': int(os.getenv('DORIS_STREAM_LOAD_PORT', '18040')),  # BE HTTP 端口
    'user': 'root',
    'password': ''
}

# 默认 LLM 资源名称
DEFAULT_LLM_RESOURCE = 'default_llm'

# API 配置
API_HOST = '0.0.0.0'
API_PORT = 8000

# 上传文件限制
MAX_UPLOAD_SIZE = 1024 * 1024 * 1024  # 1GB

# 同步与 Stream Load 限制
DORIS_MAX_COLUMNS = int(os.getenv('DORIS_MAX_COLUMNS', '1024'))
SYNC_BASE_CHUNK_ROWS = int(os.getenv('SYNC_BASE_CHUNK_ROWS', '200000'))
SYNC_MIN_CHUNK_ROWS = int(os.getenv('SYNC_MIN_CHUNK_ROWS', '1000'))
SYNC_MAX_CELLS = int(os.getenv('SYNC_MAX_CELLS', '50000000'))
STREAM_LOAD_BATCH_ROWS = int(os.getenv('STREAM_LOAD_BATCH_ROWS', '50000'))
STREAM_LOAD_MAX_BYTES = int(os.getenv('STREAM_LOAD_MAX_BYTES', str(256 * 1024 * 1024)))
STREAM_LOAD_TIMEOUT = int(os.getenv('STREAM_LOAD_TIMEOUT', '600'))

# 数据库连接超时配置（秒）
DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '60'))
DB_READ_TIMEOUT = int(os.getenv('DB_READ_TIMEOUT', '600'))  # 10分钟，支持大表同步
DB_WRITE_TIMEOUT = int(os.getenv('DB_WRITE_TIMEOUT', '60'))

