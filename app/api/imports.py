# app/api/imports.py
from fastapi import APIRouter, UploadFile, File
from app.models.db import Job, User
from typing import Optional
from app.tasks.import_tasks import import_valid_rows_task  # Import at top for clarity

router = APIRouter(prefix="/api/import", tags=["import"])


async def get_dev_user() -> User:
    user, _ = await User.get_or_create(
        email="dev@local.test",
        defaults={"hashed_password": "dev"}
    )
    return user


@router.post("/{object_type}")
async def upload_csv(
    object_type: str,
    file: UploadFile = File(...),
    mapping_id: Optional[int] = None  # ← NEW optional query param
):
    user = await get_dev_user()

    job = await Job.create(
        user=user,
        object_type=object_type,
        status="queued",
        meta={
            "filename": file.filename,
            "content_type": file.content_type,
            "mapping_id": mapping_id  # ← Save for later
        }
    )

    content = await file.read()

    import_valid_rows_task.delay(
        job_id=job.id,
        csv_content=content.decode("utf-8-sig"),
        object_type=object_type
    )

    return {
        "job_id": job.id,
        "status": "queued",
        "message": "CSV accepted – processing started in background"
    }


@router.get("/debug")
async def debug():
    jobs = await Job.all().prefetch_related("rows")
    return [{"job_id": j.id, "status": j.status, "rows": len(j.rows)} for j in jobs]