# app/tasks/import_tasks.py
import os
import asyncio
import io
import csv
from celery import Celery
from tortoise import Tortoise
import logging

from app.models.db import Job, JobRow
from app.models import TORTOISE_ORM
from app.schemas.validators import VALIDATORS
from app.core.qbo import get_qbo_client
logger = logging.getLogger(__name__)
celery_app = Celery(
    "qb_loader",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

celery_app.conf.update(task_track_started=True)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def import_valid_rows_task(self, job_id: int, csv_content: str, object_type: str):
    """
    Full end-to-end task:
    1. Parse CSV
    2. Save raw rows
    3. Validate using VALIDATORS[object_type]
    4. Import valid rows to QuickBooks Online
    """
    async def run():
        await Tortoise.init(config=TORTOISE_ORM)

        job = await Job.get(id=job_id)
        job.status = "parsing"
        await job.save()

        # === 1. Parse CSV ===
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)

        # Save raw rows
        job_rows = []
        for idx, row in enumerate(rows, start=2):  # row 1 = header
            job_rows.append(
                JobRow(
                    job=job,
                    row_number=idx,
                    raw_data=row,
                    status="pending",
                )
            )
        await JobRow.bulk_create(job_rows)

        job.meta["row_count"] = len(rows)
        job.status = "validating"
        await job.save()

        # === 2. Validate ===
        validator_class = VALIDATORS.get(object_type)
        if not validator_class:
            logger.error(f"No validator found for {object_type}")
            job.status = "error"
            job.meta["error"] = f"No validator found for object_type '{object_type}'"
            await job.save()
            return

        valid_count = 0
        for jrow in await job.rows.all():
            logger.info(f"Validating row {jrow.row_number}: {jrow.raw_data}")
            try:
                validated = validator_class(**jrow.raw_data)
                logger.info(f"Row {jrow.row_number} VALID")
                jrow.payload = validated.model_dump(exclude_unset=True, exclude_none=True)
                jrow.status = "valid"
                valid_count += 1
            except Exception as e:
                logger.exception(f"Validation FAILED for row {jrow.row_number}: {type(e).__name__}: {e}")
                jrow.status = "error"
                jrow.error = f"{type(e).__name__}: {e}"
            await jrow.save()

        job.meta["valid_count"] = valid_count
        job.status = "importing"
        await job.save()

        # === 3. Import to QBO ===
        user = await job.user  # ← this fetches the actual User instance
        client = await get_qbo_client(user)        
        success = 0

        valid_rows = await job.rows.filter(status="valid")
        for row in valid_rows:
            payload_to_send = row.payload
            

            logger.info(f"Attempting to create {object_type} for row {row.row_number} with payload: {payload_to_send}")

            try:
                # Add ?minorversion=75 for latest API behavior (current as of Dec 2025)
                resp = await client.post(f"/{object_type}?minorversion=75", json=payload_to_send)
                
                if resp.status_code in (200, 201):
                    qbo_obj = resp.json().get(object_type.capitalize(), {})
                    row.status = "success"
                    row.meta = {"qbo_id": qbo_obj.get("Id"), "sync_token": qbo_obj.get("SyncToken", 0)}
                    success += 1
                    logger.info(f"Successfully created {object_type} for row {row.row_number}: QBO ID {row.meta['qbo_id']}")
                else:
                    error_detail = resp.text or "No response body"
                    logger.error(f"QBO FAILED for row {row.row_number} - Status: {resp.status_code} - Response: {error_detail}")
                    logger.error(f"Failed payload was: {payload_to_send}")
                    row.status = "error"
                    row.error = f"QBO {resp.status_code}: {error_detail}"
            except Exception as e:
                logger.exception(f"Exception while importing row {row.row_number}")
                row.status = "error"
                row.error = str(e)
            await row.save()

        # Final status
        total_valid = len(valid_rows)
        job.status = "completed" if success == total_valid else "completed_with_errors"
        job.meta.update({"success_count": success, "valid_count": total_valid})
        await job.save()

        await client.aclose()
        await Tortoise.close_connections()

    # Run the async function
    asyncio.run(run())