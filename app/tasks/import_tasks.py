# app/tasks/import_tasks.py
from app.models.db import Job, JobRow
from app.utils.parser import parse_csv
import os

async def process_import_task(job_id: int, file_path: str):
    job = await Job.get(id=job_id)
    job.status = "parsing"
    await job.save()

    records = parse_csv(file_path)

    for idx, record in enumerate(records, start=2):  # row 1 = header
        await JobRow.create(
            job=job,
            row_number=idx,
            status="pending",
            raw_data=record
        )

    job.status = "parsed"
    await job.save()

    # cleanup
    os.unlink(file_path, missing_ok=True)