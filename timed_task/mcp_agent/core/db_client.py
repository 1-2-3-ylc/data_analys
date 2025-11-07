'''
Author: 1-2-3-ylc 1245936974@qq.com
Date: 2025-11-06 17:50:02
LastEditors: 1-2-3-ylc 1245936974@qq.com
LastEditTime: 2025-11-06 18:20:17
FilePath: \Project_Business\timed_task\mcp_agent\core\db_client.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import requests
from typing import Optional, Dict
from config.settings import DB_SERVICE_URL, DB_CONFIG

class DBClient:
    @staticmethod
    def execute_sql(sql: str, params: Dict = None) -> Optional[Dict]:
        """调用数据库服务执行SQL"""
        try:
            payload = {
                "db_config": DB_CONFIG,
                "sql": sql,
                "params": params or {}
            }
            response = requests.post(DB_SERVICE_URL, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()  # 格式: {"success": True, "data": ...}
        except Exception as e:
            print(f"数据库调用失败: {str(e)}")
            return None