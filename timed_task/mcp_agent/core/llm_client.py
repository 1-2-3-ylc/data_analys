'''
Author: 1-2-3-ylc 1245936974@qq.com
Date: 2025-11-07 16:45:19
LastEditors: 1-2-3-ylc 1245936974@qq.com
LastEditTime: 2025-11-07 16:48:37
FilePath: \Project_Business、timed_task\mcp_agent\core\llm_client.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
"""
大语言模型客户端，用于与LLM进行交互
"""
from typing import List, Dict, Any, Optional
from openai import OpenAI
from config.settings import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL
import json

class LLMClient:
    def __init__(self):
        self.client = OpenAI(
            api_key=LLM_API_KEY,
            base_url=LLM_BASE_URL
        )
        self.model = LLM_MODEL

    def chat_completion(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        调用LLM进行对话补全
        
        Args:
            messages: 对话历史消息
            tools: 可用工具列表
            
        Returns:
            LLM响应结果
        """
        try:
            kwargs = {
                "model": self.model,
                "messages": messages
            }
            
            if tools:
                kwargs["tools"] = tools
                kwargs["tool_choice"] = "auto"
            
            response = self.client.chat.completions.create(**kwargs)
            return response.choices[0].message.model_dump()
        except Exception as e:
            print(f"LLM调用失败: {str(e)}")
            return {"role": "assistant", "content": f"抱歉，处理请求时出现错误: {str(e)}"}

    def generate_tool_description(self) -> List[Dict]:
        """
        生成工具描述，符合MCP规范
        """
        return [
            {
                "type": "function",
                "function": {
                    "name": "ocr_tool",
                    "description": "光学字符识别工具，用于从图片中提取文字",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "image_path": {
                                "type": "string",
                                "description": "需要识别的图片文件路径"
                            }
                        },
                        "required": ["image_path"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "db_tool",
                    "description": "数据库操作工具，用于存储和查询数据",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "action": {
                                "type": "string",
                                "enum": ["create_table", "insert_data", "query_data"],
                                "description": "数据库操作类型"
                            },
                            "table_name": {
                                "type": "string",
                                "description": "表名"
                            },
                            "data": {
                                "type": "object",
                                "description": "操作数据"
                            }
                        },
                        "required": ["action", "table_name"]
                    }
                }
            }
        ]