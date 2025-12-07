# backend/app/main.py
import logging
from fastapi import FastAPI
from .routes import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI(title="PySpark Code Explainer")
app.include_router(router)

@app.get("/")
def root():
    return {"message": "PySpark LLM Explainer API running"}
