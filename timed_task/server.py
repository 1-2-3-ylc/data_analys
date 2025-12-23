
from mcp.server.fastmcp import FastMCP


# 创建一个 FastMCP 实例
mcp = FastMCP("Demo")

# 添加一个加法器
@mcp.tool()
def add(a: int, b: int) -> int:
    return a + b

# 注册一个资源（问候语）
@mcp.resource("greeting://{name}")
def greeting_resource(name: str) -> str:
    """返回个性化问候语"""
    return f"Hello, {name}!"

if __name__ == "__main__":
    # 启动服务器（默认使用 SSE 传输，端口 8000）
    mcp.run()