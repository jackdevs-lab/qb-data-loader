# app/api/imports.py
from fastapi import APIRouter, UploadFile, File, HTTPException
import csv
import io
from app.models.db import Job, User
from app.tasks.import_tasks import import_valid_rows_task

router = APIRouter(prefix="/api/import", tags=["import"])

# Temporary dev user until Clerk is added
async def get_dev_user() -> User:
    user, _ = await User.get_or_create(
        email="dev@local.test",
        defaults={"hashed_password": "dev"}
    )
    return user

# Step 1: Upload CSV → Parse headers → Store in job
@router.post("/{object_type}")
async def upload_csv_for_mapping(
    object_type: str,
    file: UploadFile = File(...)
):
    user = await get_dev_user()

    content = await file.read()
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames

    if not headers:
        raise HTTPException(status_code=400, detail="CSV has no headers or is empty")

    # Count rows (rewind reader)
    rows = list(reader)
    row_count = len(rows) + 1  # +1 for header

    job = await Job.create(
        user=user,
        object_type=object_type,
        status="uploaded",
        meta={
            "filename": file.filename,
            "headers": headers,
            "row_count": row_count,
            "csv_content": text  # Store full CSV for later import
        }
    )

    return {
        "job_id": job.id,
        "headers": headers,
        "row_count": row_count
    }

# Step 2: Start import with user-defined mapping
@router.post("/{object_type}/{job_id}/start")
async def start_import_with_mapping(
    object_type: str,
    job_id: int,
    mapping: dict[str, str]  # {"CSV Header": "QBO Field"}
):
    user = await get_dev_user()
    job = await Job.get_or_none(id=job_id, user=user)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "uploaded":
        raise HTTPException(status_code=400, detail="Job already processing or completed")

    # Enforce required field
    if "DisplayName" not in mapping.values():
        raise HTTPException(status_code=400, detail="DisplayName is required — please map a column to it")

    # Save mapping and start import
    job.meta["mapping"] = mapping
    job.status = "queued"
    await job.save()

    import_valid_rows_task.delay(
        job_id=job.id,
        csv_content=job.meta["csv_content"],
        object_type=object_type
    )

    return {"message": "Import started successfully!"}

# Keep debug endpoint
@router.get("/debug")
async def debug():
    jobs = await Job.all().prefetch_related("rows")
    return [{"job_id": j.id, "status": j.status, "rows": len(j.rows)} for j in jobs]