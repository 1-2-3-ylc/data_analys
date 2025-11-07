from typing import Dict, List, Tuple
import re

class TextParser:
    @staticmethod
    def extract_entities(text: str) -> Dict[str, List[str]]:
        """从文本中提取实体（简单示例，可根据实际需求扩展）"""
        # 示例：提取邮箱、电话、日期
        entities = {
            "emails": re.findall(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", text),
            "phones": re.findall(r"\b\d{11}\b", text),
            "dates": re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text),
            "raw_text": [text]  # 保留原始文本
        }
        return entities

    @staticmethod
    def generate_table_structure(entities: Dict[str, List[str]], table_name: str) -> Tuple[str, Dict]:
        """根据实体生成表结构SQL和插入数据"""
        # 生成表结构（示例字段，可动态扩展）
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            raw_text TEXT,
            emails TEXT,
            phones TEXT,
            dates TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # 处理插入数据
        insert_data = {
            "table_name": table_name,
            "raw_text": "\n".join(entities["raw_text"]),
            "emails": ",".join(entities["emails"]),
            "phones": ",".join(entities["phones"]),
            "dates": ",".join(entities["dates"])
        }
        return create_sql.strip(), insert_data

    @staticmethod
    def format_entities_for_llm(entities: Dict[str, List[str]]) -> str:
        """
        格式化实体信息供LLM使用
        """
        formatted = []
        for entity_type, values in entities.items():
            if values:
                formatted.append(f"{entity_type.capitalize()}: {', '.join(values)}")
        return "\n".join(formatted) if formatted else "未检测到特定实体"