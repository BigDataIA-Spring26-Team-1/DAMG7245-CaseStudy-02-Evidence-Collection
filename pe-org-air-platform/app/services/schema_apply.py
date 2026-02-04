from __future__ import annotations

from pathlib import Path

from app.services.snowflake import get_snowflake_connection


def _split_sql_statements(sql: str) -> list[str]:
    """
    Splits a SQL script into individual statements.
    Assumes statements are separated by semicolons.
    """
    statements = []
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)
    return statements


def apply_schema() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "database" / "schema.sql"

    sql = schema_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql)

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        for stmt in statements:
            cur.execute(stmt)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    apply_schema()
    print("Snowflake schema applied successfully")
