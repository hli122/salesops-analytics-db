from fastapi import FastAPI
from app.routers import reports

app = FastAPI(title="SalesOps API", version="0.1")

@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(reports.router)
