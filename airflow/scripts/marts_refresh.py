from connect_db import get_conn

def refresh_materialized_view(schema_name_mv: str):
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"refresh materialized view {schema_name_mv};")
    finally:
        conn.close()