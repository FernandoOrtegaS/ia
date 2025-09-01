from mcp.server.fastmcp import FastMCP
import os
import psycopg
from psycopg.rows import dict_row
from langchain_aws import ChatBedrockConverse
from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP()

DATABASE_URL = os.getenv("DATABASE_URL")

llm = ChatBedrockConverse(
    model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    region_name=os.getenv("AWS_REGION", "us-east-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

@mcp.tool()
def pg_query(sql_text: str, params: dict | None = None, limit: int = 100) -> dict:
    """
    Ejecuta una consulta SELECT segura contra PostgreSQL usando DATABASE_URL del .env.
    Además, permite que Claude reescriba consultas SQL si se solicita.
    """
    if not DATABASE_URL:
        return {"ok": False, "error": "DATABASE_URL no está definida en .env"}

    if not sql_text or not sql_text.strip().lower().startswith("select"):
        return {"ok": False, "error": "Solo se permiten consultas SELECT"}

    # agrega LIMIT si no existe uno explícito
    add_limit = " limit " not in sql_text.strip().lower()
    query = sql_text + (f" LIMIT {int(limit)}" if add_limit else "")

    try:
        with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                rows = cur.fetchall()
        return {"ok": True, "rows": rows, "meta": {"applied_limit": add_limit, "limit": limit}}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

@mcp.tool()
def ask_claude(prompt: str) -> dict:
    """
    Envía un prompt a Claude en AWS Bedrock y devuelve la respuesta.
    """
    try:
        response = llm.invoke(prompt)
        return {"ok": True, "response": response.content[0].text}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

if __name__ == "__main__":
    mcp.run()