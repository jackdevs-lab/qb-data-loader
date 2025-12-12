# app/main.py
from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.models.db import TORTOISE_ORM
from app.api.jobs import router as jobs_router
from app.api.imports import router as import_router
app = FastAPI(title="QB Data Loader")
app.include_router(import_router)
app.include_router(jobs_router)
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