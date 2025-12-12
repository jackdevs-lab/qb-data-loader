# app/tasks/import_tasks.py
import os
import asyncio
from celery import Celery
from tortoise import Tortoise

from app.models.db import Job, JobRow
from app.models import TORTOISE_ORM
from app.utils.parser import parse_csv

# ←←← THIS IS THE LINE CELERY LOOKS FOR ←←←
celery_app = Celery(
    "qb_loader",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

# Optional: make it visible in logs
celery_app.conf.update(task_track_started=True)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_import_task(self, job_id: int, file_path: str):
    """Real background task – runs in Celery worker"""
    async def run():
        await Tortoise.init(config=TORTOISE_ORM)
        job = await Job.get(id=job_id)

        job.status = "parsing"
        await job.save()

        records = parse_csv(file_path)
        for idx, record in enumerate(records, start=2):
            await JobRow.create(
                job=job,
                row_number=idx,
                status="pending",
                raw_data=record,
            )

        job.status = "parsed"
        await job.save()

        # cleanup temp file
        try:
            os.unlink(file_path)
        except FileNotFoundError:
            pass

        await Tortoise.close_connections()

    # This runs the async code inside the sync Celery task
    asyncio.run(run())