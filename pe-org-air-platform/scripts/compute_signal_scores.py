from __future__ import annotations

import json
import re
from typing import Dict
from app.services.snowflake import get_snowflake_connection


def score_jobs(n: int) -> float:
    return min(100.0, (n / 50.0) * 100.0)


def score_news(n: int) -> float:
    return min(100.0, (n / 40.0) * 100.0)


def score_tech(unique_keywords: int) -> float:
    return min(100.0, (unique_keywords / 10.0) * 100.0)


def score_patents(n: int) -> float:
    return min(100.0, (n / 20.0) * 100.0)


def extract_rss_item_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"<item>", text, re.IGNORECASE))


def normalize_metadata(metadata) -> Dict:
    if metadata is None:
        return {}
    if isinstance(metadata, str):
        return json.loads(metadata)
    return dict(metadata)


def main() -> int:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id, signal_type, content_text, metadata
            FROM external_signals
        """)

        rows = cur.fetchall()

        for sid, signal_type, content_text, metadata in rows:
            meta = normalize_metadata(metadata)

            # Skip if already scored
            if "score" in meta:
                continue

            score = 0.0

            if signal_type == "jobs":
                count = extract_rss_item_count(content_text)
                meta["count"] = count
                score = score_jobs(count)

            elif signal_type == "news":
                count = extract_rss_item_count(content_text)
                meta["count"] = count
                score = score_news(count)

            elif signal_type == "tech":
                counts = meta.get("counts", {})
                unique = len([k for k, v in counts.items() if v > 0])
                meta["unique_keywords"] = unique
                score = score_tech(unique)

            elif signal_type == "patents":
                count = extract_rss_item_count(content_text)
                meta["count"] = count
                score = score_patents(count)

            meta["score"] = round(score, 2)

            cur.execute(
                """
                UPDATE external_signals
                SET metadata = PARSE_JSON(%s)
                WHERE id = %s
                """,
                (json.dumps(meta), sid),
            )

        print("✅ Signal-level scoring completed")
        return 0

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
