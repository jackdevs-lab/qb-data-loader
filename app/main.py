from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
#from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
from app.core.db import TORTOISE_ORM
from app.api.jobs import router as jobs_router
from app.api.auth_qbo import router as qbo_auth_router
from app.api.imports import router as import_router
from app.api.mappings import router as mappings_router
from app.api.reports import router as reports_router
from app.api.customers import router as customers_router
from app.api.products import router as products_router
#rom app.api.ai import router as ai_router  # NEW
from app.api.sync import router as sync_router  # NEW
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
app = FastAPI(title="Dream QBO Lite")
app.include_router(import_router)
app.include_router(jobs_router)
app.include_router(qbo_auth_router, prefix="/api/auth_qbo")
app.include_router(mappings_router, prefix="/api/mappings")
app.include_router(reports_router)
app.include_router(customers_router)
app.include_router(products_router)
#app.include_router(ai_router)  # NEW
app.include_router(sync_router)  # NEW
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update for prod
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
