# app/api/jobs.py ← REPLACE EVERYTHING WITH THIS
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app.models.db import Job, JobRow   # ← correct import for your project
import csv
import io

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

# Temporary: act as user_id = 1 (no real auth yet)
CURRENT_USER_ID = 1

@router.get("")
async def list_jobs():
    jobs = await Job.filter(user_id=CURRENT_USER_ID).order_by("-created_at").prefetch_related("rows")
    result = []
    for j in jobs:
        total = len(j.rows)
        failed = await j.rows.filter(status="error").count() if total else 0
        result.append({
            "id": j.id,
            "object_type": j.object_type,
            "status": j.status,
            "created_at": j.created_at.isoformat(),
            "filename": j.meta.get("original_filename", "unknown.csv"),
            "total_rows": total,
            "failed_rows": failed,
        })
    return result

@router.get("/{job_id}")
async def get_job(job_id: int):
    job = await Job.get_or_none(id=job_id, user_id=CURRENT_USER_ID).prefetch_related("rows")
    if not job:
        raise HTTPException(404, "Job not found")
    total = len(job.rows)
    return {
        "id": job.id,
        "status": job.status,
        "object_type": job.object_type,
        "created_at": job.created_at.isoformat(),
        "meta": job.meta,
        "progress": {
            "total": total,
            "valid": await job.rows.filter(status="valid").count(),
            "error": await job.rows.filter(status="error").count(),
        }
    }

@router.get("/{job_id}/errors")
async def download_errors(job_id: int):
    job = await Job.get_or_none(id=job_id, user_id=CURRENT_USER_ID).prefetch_related("rows")
    if not job:
        raise HTTPException(404, "Job not found")

    errors = await job.rows.filter(status="error").values("row_number", "raw_data", "error")
    if not errors:
        return {"message": "No errors – all rows valid!"}

    output = io.StringIO()
    writer = csv.writer(output)
    headers = ["Row #", "Error"] + list(errors[0]["raw_data"].keys())
    writer.writerow(headers)
    for e in errors:
        row = [e["row_number"], e["error"]] + [e["raw_data"].get(k, "") for k in errors[0]["raw_data"].keys()]
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=job_{job_id}_errors.csv"}
    )