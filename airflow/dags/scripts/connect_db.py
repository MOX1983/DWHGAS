import psycopg2

from config import HOST, OPTIONS_WRITE, PORT, POSTGRES_DB


def delete_by_logical_date(name_table: str, logical_date: str):
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=POSTGRES_DB,
        user=OPTIONS_WRITE["user"],
        password=OPTIONS_WRITE["password"],
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {name_table} WHERE logical_date = %s", (logical_date,)
            )
    finally:
        conn.close()
