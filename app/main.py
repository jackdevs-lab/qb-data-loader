# app/main.py
from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.models.db import TORTOISE_ORM
from app.api.jobs import router as jobs_router
from app.api.auth_qbo import router as qbo_auth_router
from app.api.imports import router as import_router
from app.api.mappings import router as mappings_router
app = FastAPI(title="QB Data Loader")
app.include_router(import_router)
app.include_router(jobs_router)
app.include_router(qbo_auth_router, prefix="/auth")
app.include_router(mappings_router, prefix="/api/mappings")
@app.get("/")
async def root():
    return {"message": "QB Data Loader is running – database ready!"}


register_tortoise(
    app,
    config=TORTOISE_ORM,
    generate_schemas=True,   
    add_exception_handlers=True,
)