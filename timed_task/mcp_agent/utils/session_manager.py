# '''
# Author: 1-2-3-ylc 1245936974@qq.com
# Date: 2025-11-06 17:51:52
# LastEditors: 1-2-3-ylc 1245936974@qq.com
# LastEditTime: 2025-11-06 18:11:27
# FilePath: \Project_Business/timed_task\mcp_agent\utils\session_manager.py
# Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
# '''
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class Session:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = datetime.now()
        self.last_active = datetime.now()
        self.context: Dict[str, Any] = {
            "history": [],  # 对话历史
            "tool_calls": []  # 工具调用记录
        }
    # 判断会话是否过期
    def is_expired(self, expiry_seconds: int) -> bool:
        return datetime.now() - self.last_active > timedelta(seconds=expiry_seconds)

    # 更新会话活动时间
    def update_activity(self):
        self.last_active = datetime.now()

class SessionManager:
    def __init__(self, expiry_seconds: int):
        self.expiry_seconds = expiry_seconds
        # 初始化字典，用于存储会话
        self.sessions: Dict[str, Session] = {}
        self.lock = threading.Lock()  # 多线程安全锁，互斥锁

    def create_session(self) -> str:
        # 创建会话，并返回会话ID
        with self.lock:
            '''
            with 语句确保锁的自动管理：
            进入代码块时自动调用 self.lock.acquire()
            退出代码块时自动调用 self.lock.release()
            即使发生异常也会正确释放锁
            '''
            session_id = f"session_{id(datetime.now())}"  # 简单生成唯一ID
            self.sessions[session_id] = Session(session_id)
            return session_id

    def get_session(self, session_id: str) -> Optional[Session]:
        with self.lock:
            session = self.sessions.get(session_id)
            # 检查会话是否存在、过期
            if session and not session.is_expired(self.expiry_seconds):
                session.update_activity()
                return session
            # 清理过期会话
            if session:
                del self.sessions[session_id]
            return None

    def update_session_context(self, session_id: str, key: str, value: Any):
        # 通过session_id获取对应的会话对象
        session = self.get_session(session_id)
        if session:
            # 会话存在，则将指定的键值对存储到该会话的context属性中
            session.context[key] = value

    def append_session_history(self, session_id: str, role: str, content: str):
        session = self.get_session(session_id)
        if session:
            # 如果会话存在，则在该会话的上下文历史记录中追加一条包含角色、内容和时间戳的新记录
            session.context["history"].append({
                "role": role,
                "content": content,
                "timestamp": datetime.now().isoformat()
            })