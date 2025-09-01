## Agente NL→SQL con Claude (AWS Bedrock) y SQLAlchemy

### Requisitos
- Python 3.10+
- Credenciales AWS con acceso a Bedrock (modelo `anthropic.claude-sonnet-4-20250514-v1:0` en región `us-east-2`).
- Base de datos PostgreSQL accesible vía `DATABASE_URL`.

### Instalación
1. Crear y activar venv (opcional):
```bash
python -m venv venv
venv\\Scripts\\activate
```
2. Instalar dependencias:
```bash
pip install -r requirements.txt
```
3. Crear archivo `.env` con:
```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-2
DATABASE_URL=postgresql+psycopg://user:password@host:port/dbname
```

### Uso
Ejecutar:
```bash
python main.py
```
- Escribe preguntas en lenguaje natural. Ej.: "¿Cuántos usuarios hay por país?"
- Comandos especiales: "salir" o "exit".

### Seguridad y validaciones
- Solo se permiten consultas `SELECT` (se bloquea DML/DDL).
- Se aplica `LIMIT` automático si no existe.
- Validación previa (LIMIT 0) para detectar errores tempranos.
- El esquema se obtiene de `information_schema` y se usa como contexto para NL→SQL.

### Herramientas (TOOLS)
- `pg_query(sql_text, params=None, limit=100)`: ejecuta consultas seguras vía SQLAlchemy.
- Detección de tablas y columnas para asistencia estructural.

### Notas de rendimiento
- `pool_pre_ping` evita cortes de conexión.
- Uso de `future=True` en SQLAlchemy 2.x.

### Problemas comunes
- `DATABASE_URL` no definida: configure `.env`.
- Credenciales Bedrock inválidas o sin permisos: verifique IAM y región.
- Esquema distinto de `dbo`: ajuste `SET search_path` o queries del esquema.

