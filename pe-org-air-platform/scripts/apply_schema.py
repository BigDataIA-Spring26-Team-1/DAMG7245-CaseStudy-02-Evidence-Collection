from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection
 
 
def split_sql_statements(sql: str) -> list[str]:
    """
    Very simple SQL splitter: splits on semicolons.
    Works for our schema.sql because it contains plain DDL.
    """
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def strip_leading_line_comments(sql: str) -> str:
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and lines[i].lstrip().startswith("--"):
        i += 1
    return "\n".join(lines[i:]).strip()
 
 
def main() -> int:
    schema_path = ROOT / "app" / "database" / "schema.sql"
    if not schema_path.exists():
        raise SystemExit(f"schema.sql not found at: {schema_path.resolve()}")
 
    sql_text = schema_path.read_text(encoding="utf-8", errors="ignore")
    statements = split_sql_statements(sql_text)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        applied = 0
        for i, stmt in enumerate(statements, start=1):
            s = strip_leading_line_comments(stmt)
            if not s:
                continue
            try:
                cur.execute(s)
                applied += 1
            except Exception:
                print(f"\n Failed on statement #{i}:\n{s}\n")
                raise
 
        print(f"Applied {applied} SQL statements from {schema_path}")
        return 0
    finally:
        cur.close()
        conn.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())
