import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()  # 自动读取 .env

def get_engine():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing DATABASE_URL in environment or .env")
    return create_engine(database_url, future=True)
