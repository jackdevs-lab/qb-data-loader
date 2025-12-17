# app/api/imports.py

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
import csv
import io
from app.models.db import Job, User
from app.tasks.import_tasks import import_valid_rows_task
from app.core.auth import get_current_user

router = APIRouter(prefix="/api/import", tags=["import"])

@router.post("/{object_type}")
async def upload_csv_for_mapping(
    object_type: str,
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    content = await file.read()
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames

    if not headers:
        raise HTTPException(status_code=400, detail="CSV has no headers or is empty")

    rows = list(reader)
    row_count = len(rows) + 1  # +1 for header

    # NEW: Take first 50 rows for preview (safe limit to avoid huge responses)
    preview_rows = rows[:50]
    # Optional: If you want all rows for small files (<100), use rows directly
    # But 50 is a good balance for performance and usability

    job = await Job.create(
        user=user,
        object_type=object_type,
        status="uploaded",
        meta={
            "filename": file.filename,
            "headers": headers,
            "row_count": row_count,
            "csv_content": text,
            "preview_rows": preview_rows  # ← Store in meta (optional, for future use)
        }
    )

    return {
        "job_id": job.id,
        "headers": headers,
        "row_count": row_count,
        "preview_rows": preview_rows  # ← CRITICAL: Return to frontend
    }


@router.post("/{object_type}/{job_id}/start")
async def start_import_with_mapping(
    object_type: str,
    job_id: int,
    mapping: dict[str, str],
    user: User = Depends(get_current_user),
):
    job = await Job.get_or_none(id=job_id, user=user)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found or access denied")
    if job.status != "uploaded":
        raise HTTPException(status_code=400, detail="Job already processing or completed")

    if "DisplayName" not in mapping.values():
        raise HTTPException(status_code=400, detail="DisplayName is required — please map a column to it")

    job.meta["mapping"] = mapping
    job.status = "queued"
    await job.save()

    import_valid_rows_task.delay(
        job_id=job.id,
        csv_content=job.meta["csv_content"],
        object_type=object_type
    )

    return {"message": "Import started successfully!"}


@router.get("/debug")
async def debug(user: User = Depends(get_current_user)):
    jobs = await Job.filter(user=user).prefetch_related("rows")
    return [{"job_id": j.id, "status": j.status, "rows": len(j.rows)} for j in jobs]