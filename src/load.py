import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2
from prefect import task

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = "cpd_db"
TABLE = "cpd_incidents"


def create_postgres_table():
    '''Create the cpd_incidents table if it doesn't exist.'''
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )
    with conn, conn.cursor() as cur:
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE} (
                date_time TIMESTAMP,
                id INTEGER PRIMARY KEY,
                type TEXT,
                subtype TEXT,
                location TEXT,
                description TEXT,
                last_updated TIMESTAMP,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                minute INTEGER,
                second INTEGER
            )
        ''')
    conn.close()


@task
def load_into_postgres(df):
    '''Replace the table contents with the transformed data, in one transaction.'''
    create_postgres_table()

    df = df.astype({"id": "int64"})
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    # Full refresh: truncate and reload inside one transaction, so a failure never leaves the table empty
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE}"))
        df.to_sql(TABLE, conn, if_exists="append", index=False)
    engine.dispose()