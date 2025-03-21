from typing import Any
import os
from dotenv import load_dotenv
import aiomysql
from mcp.server.fastmcp import FastMCP

# 加载环境变量
load_dotenv()

# 初始化 FastMCP 服务
mcp = FastMCP("mysql_generator")

# 从环境变量读取 MySQL 配置
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
}

async def execute_sql_query(sql: str, database: str) -> str:
    """执行 SQL 语句并返回结果"""
    try:
        MYSQL_CONFIG["db"] = database
        async with aiomysql.connect(**MYSQL_CONFIG) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                
                sql_lower = sql.strip().lower()
                if sql_lower.startswith("select") or sql_lower.startswith("show"):
                    result = await cursor.fetchall()
                    
                    if sql_lower.startswith("show"):
                        # MySQL 的 SHOW只返回两列，第二列是 DDL 语句
                        return result[0][1] if result else "未找到表的 DDL"
                    
                    # 普通 SELECT 语句处理
                    columns = [col[0] for col in cursor.description]
                    rows = [dict(zip(columns, row)) for row in result]
                    return "\n".join(str(row) for row in rows)
                else:
                    await conn.commit()
                    return f"执行成功，影响行数: {cursor.rowcount}"
                    
    except Exception as e:
        return f"执行出错: {str(e)}"

@mcp.tool()
async def sql_executor(sql: str, database: str) -> str:
    """执行 SQL 语句生成测试数据
    
    Args:
        sql: 要执行的 SQL 语句 (支持 INSERT/UPDATE/DELETE/SELECT)
        database: 数据库名字
    
    环境变量配置:
        MYSQL_HOST: 数据库地址 (默认 localhost)
        MYSQL_PORT: 数据库端口 (默认 3306)
        MYSQL_USER: 数据库用户 (默认 root)
        MYSQL_PASSWORD: 数据库密码
    """
    return await execute_sql_query(sql, database)

@mcp.tool()
async def get_table_ddl(table_name: str, database: str) -> str:
    """获取指定表的 DDL 语句
    
    Args:
        table_name: 表名
        database: 数据库名字
    
    Returns:
        DDL 语句 (CREATE TABLE 语句)
    """
    sql = f"SHOW CREATE TABLE `{table_name}`"
    result = await execute_sql_query(sql, database)
    return result

if __name__ == "__main__":
    mcp.run(transport="stdio")