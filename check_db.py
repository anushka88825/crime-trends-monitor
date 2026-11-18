import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

url = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@localhost:5433/cpd_db"
)
engine = create_engine(url)
print(pd.read_sql("SELECT COUNT(*) AS n, MAX(date_time) AS latest FROM cpd_incidents", engine))