import os

# 服务配置
OCR_SERVICE_URL = "http://localhost:8002/ocr"  # OCR服务接口
DB_SERVICE_URL = "http://localhost:8001/mysql"  # 数据库服务接口
SSE_SERVER_PORT = 8000  # 智能体SSE服务端口

# LLM配置
LLM_MODEL = "gpt-4"  # 或其他模型
LLM_API_KEY = "your-api-key-here"
LLM_BASE_URL = "https://api.openai.com/v1"  # 或其他API端点

# MCP配置
MCP_MAX_TOOLS = 10
MCP_CONTEXT_WINDOW = 4096

# 数据库连接配置（供DB工具使用）
DB_CONFIG = {
    "host": "192.168.1.253",
    "port": 3306,
    "user": "wxz",
    "password": "Hzhs5beGMRZmNLBMj33N",
    "database": "db_xq",
    "charset": "utf8mb4"
}

# 会话配置
SESSION_EXPIRY_SECONDS = 3600  # 会话有效期1小时