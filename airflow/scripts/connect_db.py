import psycopg2

from testPlug import db

def get_conn():
    # использовать хуки?
    return psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.dbname,
        user=db.user,
        password=db.password,
    )


def delete_by_logical_date(name_table: str, logical_date: str):
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {name_table} WHERE logical_date = %s", (logical_date,)
            )
    finally:
        conn.close()
