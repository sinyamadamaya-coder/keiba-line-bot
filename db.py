"""
データベース操作モジュール
PostgreSQL接続・血統/騎手成績の読み書き
"""
import os
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager

DATABASE_URL = os.environ.get("DATABASE_URL", "")

@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    """初回起動時にテーブルを作成"""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("[DB] テーブル初期化完了")

def get_sire_stats(sire_name, generation, surface, distance, place=None):
    """父馬/母父馬の条件別成績を取得"""
    sql = """
        SELECT wins, second, third, out_count
        FROM sire_stats
        WHERE sire_name = %s AND generation = %s AND surface = %s
          AND distance = %s AND place IS NOT DISTINCT FROM %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (sire_name, generation, surface, distance, place))
            row = cur.fetchone()
    if not row:
        return None
    return {"wins": row[0], "second": row[1], "third": row[2], "out": row[3]}

def upsert_sire_stats(records):
    """血統成績を一括UPSERT（updated_atはDB側でCURRENT_TIMESTAMPを使用）"""
    sql = """
        INSERT INTO sire_stats
            (sire_name, generation, surface, distance, place, wins, second, third, out_count)
        VALUES %s
        ON CONFLICT (sire_name, generation, surface, distance, place)
        DO UPDATE SET
            wins      = EXCLUDED.wins,
            second    = EXCLUDED.second,
            third     = EXCLUDED.third,
            out_count = EXCLUDED.out_count,
            updated_at = CURRENT_TIMESTAMP
    """
    values = [(
        r["sire_name"], r["generation"], r["surface"],
        r["distance"], r.get("place"),
        r["wins"], r["second"], r["third"], r["out_count"]
    ) for r in records]
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

def get_jockey_stats(jockey_name, surface, distance, place=None):
    """騎手の条件別成績を取得"""
    sql = """
        SELECT wins, second, third, out_count
        FROM jockey_stats
        WHERE jockey_name = %s AND surface = %s
          AND distance = %s AND place IS NOT DISTINCT FROM %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (jockey_name, surface, distance, place))
            row = cur.fetchone()
    if not row:
        return None
    return {"wins": row[0], "second": row[1], "third": row[2], "out": row[3]}

def upsert_jockey_stats(records):
    """騎手成績を一括UPSERT（updated_atはDB側でCURRENT_TIMESTAMPを使用）"""
    sql = """
        INSERT INTO jockey_stats
            (jockey_name, surface, distance, place, wins, second, third, out_count)
        VALUES %s
        ON CONFLICT (jockey_name, surface, distance, place)
        DO UPDATE SET
            wins      = EXCLUDED.wins,
            second    = EXCLUDED.second,
            third     = EXCLUDED.third,
            out_count = EXCLUDED.out_count,
            updated_at = CURRENT_TIMESTAMP
    """
    values = [(
        r["jockey_name"], r["surface"], r["distance"], r.get("place"),
        r["wins"], r["second"], r["third"], r["out_count"]
    ) for r in records]
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)

def mark_race_processed(race_id, race_date, place, surface, distance):
    sql = """
        INSERT INTO race_results (race_id, race_date, place, surface, distance, processed)
        VALUES (%s, %s, %s, %s, %s, TRUE)
        ON CONFLICT (race_id) DO UPDATE SET processed = TRUE
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (race_id, race_date, place, surface, distance))

def get_unprocessed_race_ids():
    sql = "SELECT race_id FROM race_results WHERE processed = FALSE ORDER BY race_date"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [row[0] for row in cur.fetchall()]

def stats_to_str(stats):
    if not stats:
        return None
    total = stats["wins"] + stats["second"] + stats["third"] + stats["out"]
    if total == 0:
        return None
    return f"{stats['wins']}-{stats['second']}-{stats['third']}-{stats['out']}"
