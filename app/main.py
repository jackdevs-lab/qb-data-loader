# app/main.py
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.core.db import TORTOISE_ORM
from app.api.jobs import router as jobs_router
from app.api.auth_qbo import router as qbo_auth_router
from app.api.imports import router as import_router
from app.api.mappings import router as mappings_router
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
app = FastAPI(title="QB Data Loader")
app.include_router(import_router)
app.include_router(jobs_router)
app.include_router(qbo_auth_router, prefix="/api/auth_qbo.py")
app.include_router(mappings_router, prefix="/api/mappings")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def root():
    return FileResponse("frontend/index.html")
register_tortoise(
    app,
    config=TORTOISE_ORM,
    generate_schemas=False,   
    add_exception_handlers=True,
)