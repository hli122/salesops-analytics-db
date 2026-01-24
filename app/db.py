import os
from sqlalchemy import create_engine

def get_engine():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "Missing DATABASE_URL. In PowerShell set:\n"
            '$env:DATABASE_URL="postgresql+psycopg2://user:pass@localhost:5432/salesops"'
        )
    return create_engine(database_url, future=True)
