# app/api/imports.py
from fastapi import APIRouter, UploadFile, File, BackgroundTasks
from app.models.db import Job, JobRow, User
import csv
import io

router = APIRouter(prefix="/api/import", tags=["import"])


async def get_dev_user() -> User:
    user, _ = await User.get_or_create(
        email="dev@local.test",
        defaults={"hashed_password": "dev"}
    )
    return user


@router.post("/{object_type}")
async def upload_csv(
    object_type: str,                    # "customer", "invoice", etc.
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    user = await get_dev_user()

    # Create the Job
    job = await Job.create(
        user=user,
        object_type=object_type,
        status="parsing",
        meta={"filename": file.filename, "content_type": file.content_type}
    )

    # Parse CSV in background so endpoint returns instantly
    background_tasks.add_task(parse_and_save_rows, job.id, await file.read())

    return {"job_id": job.id, "status": "queued", "message": "CSV accepted – parsing started"}


async def parse_and_save_rows(job_id: int, content: bytes):
    job = await Job.get(id=job_id)
    job.status = "parsing"
    await job.save()

    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))  # handles BOM
    rows = list(reader)

    job_rows = []
    for idx, row in enumerate(rows, start=2):  # start=2 because row 1 = header
        job_rows.append(JobRow(
            job=job,
            row_number=idx,
            raw_data=row,
            status="pending"
        ))

    await JobRow.bulk_create(job_rows)

    job.status = "parsed"
    job.meta["row_count"] = len(rows)
    await job.save()
@router.get("/debug")
async def debug():
    jobs = await Job.all().prefetch_related("rows")
    return [{"job_id": j.id, "status": j.status, "rows": len(j.rows)} for j in jobs]