from typing import Dict, Optional, List, Any
from .ocr_client import OCRClient
from .db_client import DBClient
from .llm_client import LLMClient
from utils.text_parser import TextParser
from utils.session_manager import SessionManager
from config.settings import SESSION_EXPIRY_SECONDS
import json

class MCPAgent:
    def __init__(self):
        self.session_manager = SessionManager(SESSION_EXPIRY_SECONDS)
        self.ocr_client = OCRClient()
        self.db_client = DBClient()
        self.llm_client = LLMClient()
        self.parser = TextParser()

    def create_session(self) -> str:
        """创建新会话"""
        return self.session_manager.create_session()

    def process_request(self, session_id: str, user_query: str, image_path: Optional[str] = None) -> Dict:
        """处理用户请求（主入口）"""
        session = self.session_manager.get_session(session_id)
        if not session:
            return {"status": "error", "message": "会话已过期，请重新创建"}

        # 记录用户查询
        self.session_manager.append_session_history(session_id, "user", user_query)

        # 构建对话历史
        messages = self._build_conversation_history(session)
        
        # 添加当前用户消息
        messages.append({"role": "user", "content": user_query})
        
        # 获取工具描述
        tools = self.llm_client.generate_tool_description()
        
        # 调用LLM进行决策
        llm_response = self.llm_client.chat_completion(messages, tools)
        
        # 处理LLM响应
        return self._handle_llm_response(session_id, llm_response, image_path)

    def _build_conversation_history(self, session) -> List[Dict[str, str]]:
        """
        构建对话历史上下文
        """
        history = []
        for item in session.context["history"]:
            history.append({
                "role": item["role"],
                "content": item["content"]
            })
        return history

    def _handle_llm_response(self, session_id: str, llm_response: Dict, image_path: Optional[str]) -> Dict:
        """
        处理LLM响应，包括工具调用
        """
        # 记录助手响应
        self.session_manager.append_session_history(
            session_id, 
            llm_response["role"], 
            llm_response.get("content", "")
        )

        # 检查是否有工具调用
        if llm_response.get("tool_calls"):
            # 处理工具调用
            tool_results = self._execute_tool_calls(session_id, llm_response["tool_calls"], image_path)
            
            # 将工具调用结果添加到对话历史
            messages = self._build_conversation_history(self.session_manager.get_session(session_id))
            messages.extend(tool_results)
            
            # 再次调用LLM获取最终响应
            final_response = self.llm_client.chat_completion(messages)
            
            # 记录最终响应
            self.session_manager.append_session_history(
                session_id, 
                final_response["role"], 
                final_response.get("content", "")
            )
            
            return {
                "status": "success",
                "message": final_response.get("content", ""),
                "tool_calls": llm_response["tool_calls"]
            }
        else:
            # 直接返回LLM响应
            return {
                "status": "success",
                "message": llm_response.get("content", "处理完成")
            }

    def _execute_tool_calls(self, session_id: str, tool_calls: List[Dict], image_path: Optional[str]) -> List[Dict]:
        """
        执行工具调用
        """
        results = []
        
        for tool_call in tool_calls:
            function_name = tool_call["function"]["name"]
            arguments = json.loads(tool_call["function"]["arguments"])
            
            if function_name == "ocr_tool":
                # 调用OCR工具
                ocr_result = self._call_ocr_tool(session_id, image_path or arguments.get("image_path", ""))
                if ocr_result and ocr_result.get("success"):
                    results.append({
                        "tool_call_id": tool_call["id"],
                        "role": "tool",
                        "name": function_name,
                        "content": json.dumps(ocr_result)
                    })
                    
            elif function_name == "db_tool":
                # 调用数据库工具
                action = arguments.get("action")
                table_name = arguments.get("table_name")
                data = arguments.get("data", {})
                
                if action == "create_table":
                    # 创建表逻辑
                    create_sql = data.get("sql", "")
                    db_result = self.db_client.execute_sql(create_sql)
                    results.append({
                        "tool_call_id": tool_call["id"],
                        "role": "tool",
                        "name": function_name,
                        "content": json.dumps(db_result or {"success": False, "error": "未知错误"})
                    })
                elif action == "insert_data":
                    # 插入数据逻辑
                    insert_sql = data.get("sql", "")
                    params = data.get("params", {})
                    db_result = self.db_client.execute_sql(insert_sql, params)
                    results.append({
                        "tool_call_id": tool_call["id"],
                        "role": "tool",
                        "name": function_name,
                        "content": json.dumps(db_result or {"success": False, "error": "未知错误"})
                    })
        
        return results

    def _call_ocr_tool(self, session_id: str, image_path: str) -> Optional[Dict]:
        """调用OCR工具并记录日志"""
        ocr_result = self.ocr_client.call_ocr(image_path)
        if ocr_result:
            self.session_manager.append_session_history(
                session_id, "tool", f"OCR调用成功: {ocr_result['text'][:50]}..."
            )
            self.session_manager.update_session_context(
                session_id, "last_ocr_result", ocr_result
            )
        return ocr_result

    def _call_db_tool(self, session_id: str, create_sql: str, insert_data: Dict) -> Dict:
        """调用数据库工具（创建表+插入数据）并记录日志"""
        # 1. 创建表
        create_result = self.db_client.execute_sql(create_sql)
        if not create_result or not create_result["success"]:
            return {"success": False, "error": "创建表失败"}

        # 2. 插入数据
        insert_sql = f"""
        INSERT INTO {insert_data['table_name']} (raw_text, emails, phones, dates)
        VALUES (%(raw_text)s, %(emails)s, %(phones)s, %(dates)s)
        """
        insert_result = self.db_client.execute_sql(insert_sql, insert_data)
        if insert_result and insert_result["success"]:
            self.session_manager.append_session_history(
                session_id, "tool", f"数据库操作成功: 插入记录ID={insert_result.get('last_insert_id')}"
            )
            return {"success": True, "last_insert_id": insert_result.get("last_insert_id")}
        return {"success": False, "error": "插入数据失败"}