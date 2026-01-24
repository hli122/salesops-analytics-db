$env:DATABASE_URL="postgresql+psycopg2://postgres:055201Ssdlhr%40@localhost:5432/salesops"
python -m uvicorn app.main:app --reload
