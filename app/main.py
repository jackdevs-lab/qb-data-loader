# app/main.py
from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.core.db import TORTOISE_ORM

app = FastAPI(title="QB Data Loader")


@app.get("/")
async def root():
    return {"message": "QB Data Loader is running – database ready!"}


# This single line does everything: connects + creates all tables automatically
register_tortoise(
    app,
    config=TORTOISE_ORM,
    generate_schemas=True,   # ← creates/updates tables on every startup (perfect for dev)
    add_exception_handlers=True,
)