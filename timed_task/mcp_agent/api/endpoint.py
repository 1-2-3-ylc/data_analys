from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
import asyncio
from core.agent import MCPAgent
from typing import AsyncGenerator, Dict
import json

router = APIRouter()
agent = MCPAgent()

@router.post("/create-session")
async def create_session() -> Dict[str, str]:
    """创建新会话"""
    session_id = agent.create_session()
    return {"session_id": session_id}

@router.post("/process")
async def process_request(request: Request) -> JSONResponse:
    """处理用户请求（MCP智能响应）"""
    try:
        data = await request.json()
        session_id = data.get("session_id")
        user_query = data.get("query", "")
        image_path = data.get("image_path")

        if not session_id:
            raise HTTPException(status_code=400, detail="缺少session_id")

        # 处理请求
        result = agent.process_request(session_id, user_query, image_path)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"处理请求时出错: {str(e)}")