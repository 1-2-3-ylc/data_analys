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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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