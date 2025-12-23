from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.endpoint import router as api_router
from config.settings import SSE_SERVER_PORT

app = FastAPI(
    title="MCP工具集成智能体",
    description="基于Model Context Protocol的智能体，集成了OCR识别和数据库操作工具",
    version="1.0.0"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    # 明确允许的前端域名
    allow_origins=["*"],
    # 2. 是否允许携带凭证（如Cookies、认证头）
    allow_credentials=True,
    # 3. 明确允许的HTTP方法（仅开放业务必需的）
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    # 4. 明确允许的请求头（仅开放业务必需的）
    allow_headers=[
        "Content-Type",  # 标准头（JSON/表单提交必需）
        "Authorization", # 认证头（如JWT Token）
        "X-Custom-Header"  # 自定义业务头（如有）
    ],
)

app.include_router(api_router, prefix="/api")

# 添加健康检查端点
@app.get("/")
async def root():
    return {
        "message": "MCP工具集成智能体服务正在运行", 
        "version": "1.0.0",
        "docs": "/docs"
    }

# 添加MCP协议相关信息端点
@app.get("/mcp-info")
async def mcp_info():
    return {
        "protocol": "Model Context Protocol",
        "capabilities": [
            "chat_completion",
            "tool_calling",
            "context_management"
        ],
        "supported_tools": [
            "ocr_tool",
            "db_tool"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=SSE_SERVER_PORT, reload=True)