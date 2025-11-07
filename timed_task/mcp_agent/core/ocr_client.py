'''
Author: 1-2-3-ylc 1245936974@qq.com
Date: 2025-11-06 17:49:36
LastEditors: 1-2-3-ylc 1245936974@qq.com
LastEditTime: 2025-11-06 18:14:38
FilePath: \Project_Business\timed_task\mcp_agent\core\ocr_client.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import requests
from typing import Optional, Dict
from config.settings import OCR_SERVICE_URL

class OCRClient:
    @staticmethod
    def call_ocr(image_path: str) -> Optional[Dict]:
        """
        调用OCR服务识别图片文本
        这段代码定义了一个静态方法,功能是调用OCR服务识别图片中的文本。主要逻辑：
        1. 以二进制模式打开指定路径的图片文件
        2. 将图片作为文件上传到OCR服务接口:OCR_SERVICE_URL
        3. 发送POST请求并设置30秒超时
        4. 验证响应状态，返回JSON格式的识别结果
        5. 出现异常时打印错误信息并返回None
        """
        try:
            with open(image_path, "rb") as f:
                files = {"image": f}
                response = requests.post(OCR_SERVICE_URL, files=files, timeout=30)
                response.raise_for_status()
                return response.json()  # 格式: {"success": True, "text": "识别结果", "confidence": 0.9}
        except Exception as e:
            print(f"OCR调用失败: {str(e)}")
            return None