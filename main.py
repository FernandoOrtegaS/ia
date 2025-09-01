import os
import re
import json
import uuid
import datetime
import decimal
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from rich import print as rprint
# LangChain
from langchain_aws import ChatBedrockConverse
from langchain_core.prompts import ChatPromptTemplate

class SQLPlan(BaseModel):
    """Esquema Pydantic para la PRIMERA salida del LLM (plan de consulta)."""
    intent: str = Field(..., description="Resumen breve de la intención del usuario")
    sql: str = Field(..., description="Consulta SQL segura y ejecutable (solo SELECT)")
    notes: Optional[str] = Field(None, description="Notas o supuestos relevantes")

class NLQResponse(BaseModel):
    """Esquema Pydantic para la SEGUNDA salida del LLM (respuesta final al usuario)."""
    question: str = Field(..., description="La pregunta original en lenguaje natural")
    sql_used: str = Field(..., description="La consulta SQL final que se ejecutó")
    row_count: int = Field(..., description="Cantidad de filas devueltas")
    rows: List[Dict[str, Any]] = Field(..., description="Filas devueltas (limitadas)")
    summary: str = Field(..., description="Resumen/insight en lenguaje natural")

DEFAULT_MODEL_ID = "us.amazon.nova-micro-v1:0"
DEFAULT_REGION = "us-east-2"

SAFE_ROW_LIMIT = 200
SCHEMA_TABLE_LIMIT = 40
SCHEMA_COL_LIMIT = 60

def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL no está definido en el entorno")
    return create_engine(db_url, pool_pre_ping=True)

def describe_schema(engine: Engine) -> str:
    inspector = inspect(engine)
    tables = inspector.get_table_names()[:SCHEMA_TABLE_LIMIT]
    parts: List[str] = ["Esquema disponible (parcial):"]
    for t in tables:
        try:
            cols = inspector.get_columns(t)[:SCHEMA_COL_LIMIT]
            col_desc = ", ".join(f"{c['name']}:{getattr(c.get('type'), '__visit_name__', 'any')}" for c in cols)
            parts.append(f"- {t} (columnas: {col_desc})")
        except Exception:
            parts.append(f"- {t} (columnas: ?)")
    return "\n".join(parts)

def ensure_select_with_limit(sql: str) -> str:
    sql_clean = sql.strip().rstrip(";")
    if not sql_clean:
        raise ValueError("La consulta SQL está vacía")

    forbidden = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE)\b", re.IGNORECASE)
    if forbidden.search(sql_clean):
        raise ValueError("Solo se permiten consultas SELECT seguras")

    if not re.match(r"^SELECT\s", sql_clean, re.IGNORECASE):
        raise ValueError("La consulta debe iniciar con SELECT")

    if not re.search(r"\bLIMIT\s+\d+\b", sql_clean, re.IGNORECASE):
        sql_clean = f"{sql_clean} LIMIT {SAFE_ROW_LIMIT}"

    return sql_clean

def normalize_value(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value

def run_sql(engine: Engine, sql: str) -> List[Dict[str, Any]]:
    sql_safe = ensure_select_with_limit(sql)
    with engine.connect() as conn:
        result = conn.execute(text(sql_safe))
        rows = []
        for r in result:
            row_dict = {k: normalize_value(v) for k, v in dict(r._mapping).items()}
            rows.append(row_dict)
        return rows

def make_llm() -> ChatBedrockConverse:
    model_id = DEFAULT_MODEL_ID
    region = os.getenv("AWS_REGION", DEFAULT_REGION)
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_key or not aws_secret:
        raise RuntimeError("Faltan credenciales AWS en el entorno")

    return ChatBedrockConverse(
        model_id=model_id,
        region_name=region,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )


def plan_sql_from_nlq(llm: ChatBedrockConverse, question: str, schema_hint: str) -> SQLPlan:
    prompt = ChatPromptTemplate.from_messages([
        ("system", (
            "Eres un experto en SQL. Solo creas CONSULTAS SELECT seguras basadas en el esquema. "
            "Responde EXCLUSIVAMENTE en JSON que cumpla exactamente con el esquema proporcionado."
        )),
        ("human", (
            "Esquema:\n{schema}\n\n"
            "Pregunta del usuario:\n{question}\n\n"
            "Crea una consulta SQL (solo SELECT) para responder. Si es imposible, sugiere un SELECT exploratorio."
        )),
    ])

    structured_llm = llm.with_structured_output(SQLPlan, strict=True)
    chain = prompt | structured_llm
    return chain.invoke({"schema": schema_hint, "question": question})

def final_json_answer(llm: ChatBedrockConverse, question: str, sql_used: str, rows: List[Dict[str, Any]]) -> NLQResponse:
    prompt = ChatPromptTemplate.from_messages([
        ("system", (
            "Eres un analista de datos. Entregas SIEMPRE un JSON válido con el esquema solicitado. "
            "Sé conciso y fiel a los datos proporcionados."
        )),
        ("human", (
            "Pregunta original:\n{question}\n\n"
            "SQL ejecutado:\n{sql}\n\n"
            "Primeras filas (JSON):\n{rows}\n\n"
            "Genera un resumen breve y útil para negocio."
        )),
    ])

    structured_llm = llm.with_structured_output(NLQResponse, strict=True)
    chain = prompt | structured_llm
    return chain.invoke({
        "question": question,
        "sql": sql_used,
        "rows": json.dumps(rows, ensure_ascii=False),
    })

def main():
    load_dotenv()

    engine = get_engine()
    schema_hint = describe_schema(engine)
    llm = make_llm()

    rprint("[bold cyan]NLQ a SQL con Bedrock Claude (LangChain ChatBedrockConverse)\n"
           "Escribe tu pregunta en lenguaje natural. Ctrl+C para salir.[/bold cyan]")

    while True:
        try:
            question = input("\n❓ Pregunta> ").strip()
            if not question:
                continue

            try:
                plan = plan_sql_from_nlq(llm, question, schema_hint)
                rprint("[yellow]\nPlan propuesto (SQL seguro):[/yellow]")
                rprint(plan.model_dump())
            except Exception as e:
                rprint(f"[red]Error generando plan SQL: {e}[/red]")
                plan = SQLPlan(intent="Exploración", sql="SELECT * FROM information_schema.tables LIMIT 5", notes="Fallback")

            try:
                rows = run_sql(engine, plan.sql)
            except (SQLAlchemyError, ValueError) as e:
                rprint(f"[red]Error al ejecutar SQL: {e}[/red]")
                rows = []

            final_resp = final_json_answer(llm, question, plan.sql, rows)
            rprint("[green]\nRespuesta final (JSON validado):[/green]")
            print(json.dumps(final_resp.model_dump(), ensure_ascii=False, indent=2))

        except KeyboardInterrupt:
            rprint("\n[bold]Saliendo...[/bold]")
            break
        except Exception as e:
            rprint(f"[red]Error inesperado: {e}[/red]")

if __name__ == "__main__":
    main()



