# app/api/imports.py
from fastapi import APIRouter, UploadFile, File, BackgroundTasks, Depends, HTTPException
from app.models.db import Job, User
from app.tasks.import_tasks import process_import_task
from app.core.security import get_current_user  # stub for now

router = APIRouter(prefix="/api")

@router.post("/import/{object_type}")
async def upload_file(
    object_type: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user)
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only CSV for now")

    contents = await file.read()
    job = await Job.create(
        user=current_user,
        object_type=object_type,
        status="queued",
        meta={"original_filename": file.filename}
    )

    # Save file temporarily (Neon serverless has /tmp)
    import tempfile, os
    tmp_path = f"/tmp/{job.id}_{file.filename}"
    with open(tmp_path, "wb") as f:
        f.write(contents)

    # Fire and forget (Celery eager in dev → instant)
    background_tasks.add_task(process_import_task, job.id, tmp_path)

    return {"job_id": job.id, "status": "queued"}