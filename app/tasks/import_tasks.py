# app/tasks/import_tasks.py
import os
import asyncio
import io
import csv
from celery import Celery
from tortoise import Tortoise
import logging
from app.core.websocket import manager

from app.models.db import Job, JobRow, MappingTemplate
from app.core.db import TORTOISE_ORM
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
    async def run():
        await Tortoise.init(config=TORTOISE_ORM)

        job = await Job.get(id=job_id)

        async def get_progress():
            return {
                "total": await JobRow.filter(job=job).count(),
                "valid": await JobRow.filter(job=job, status="valid").count(),
                "error": await JobRow.filter(job=job, status="error").count(),
                "success": await JobRow.filter(job=job, status="success").count(),
            }

        # === Initial broadcast: parsing ===
        job.status = "parsing"
        await manager.broadcast(
            {
                "status": job.status,
                "progress": await get_progress(),
                "meta": job.meta,
            },
            job.id,
        )
        await job.save()

        # === 1. Parse CSV ===
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)

        logger.info(f"Parsed {len(rows)} rows from CSV")

        job_rows = []
        for idx, row in enumerate(rows, start=2):
            job_rows.append(
                JobRow(
                    job=job,
                    row_number=idx,
                    raw_data=row,
                    status="pending",
                )
            )
        await JobRow.bulk_create(job_rows)

        logger.info(f"Created {len(job_rows)} new JobRow entries in database")

        job.meta["row_count"] = len(rows)
        job.status = "validating"

        await manager.broadcast(
            {
                "status": job.status,
                "progress": await get_progress(),
                "meta": job.meta,
            },
            job.id,
        )
        await job.save()

        # === 2. Validate ===
        validator_class = VALIDATORS.get(object_type)
        if not validator_class:
            logger.error(f"No validator found for {object_type}")
            job.status = "error"
            job.meta["error"] = f"No validator found for object_type '{object_type}'"
            await manager.broadcast(
                {
                    "status": job.status,
                    "progress": await get_progress(),
                    "meta": job.meta,
                },
                job.id,
            )
            await job.save()
            return

        mapping = {}
        mapping_id = job.meta.get("mapping_id")
        if mapping_id:
            template = await MappingTemplate.get_or_none(id=mapping_id, user=await job.user)
            if template and template.object_type.lower() == object_type.lower():
                mapping = template.mapping

        # Auto-map first column to DisplayName if no mapping (your quick win)
        

        valid_count = 0
        current_rows = await JobRow.filter(job=job).all()

        for jrow in current_rows:
            raw = jrow.raw_data
            mapped_data = {}

            for csv_col, qbo_field in mapping.items():
                if csv_col in raw:
                    mapped_data[qbo_field] = raw[csv_col]

            for col, val in raw.items():
                if col in validator_class.model_fields and col not in mapped_data:
                    mapped_data[col] = val

            # Normalize nested fields
            if "PrimaryEmailAddr" in mapped_data and isinstance(mapped_data["PrimaryEmailAddr"], str):
                mapped_data["PrimaryEmailAddr"] = {"Address": mapped_data["PrimaryEmailAddr"].strip()}

            if "PrimaryPhone" in mapped_data and isinstance(mapped_data["PrimaryPhone"], str):
                mapped_data["PrimaryPhone"] = {"FreeFormNumber": mapped_data["PrimaryPhone"].strip()}

            if any(key.startswith("BillAddr.") for key in mapped_data):
                bill_addr = {}
                for key, val in list(mapped_data.items()):
                    if key.startswith("BillAddr."):
                        field = key.split(".", 1)[1]
                        bill_addr[field] = val
                        del mapped_data[key]
                if bill_addr:
                    mapped_data["BillAddr"] = bill_addr

            try:
                validated = validator_class(**mapped_data)
                jrow.payload = validated.model_dump(exclude_unset=True, exclude_none=True)
                jrow.status = "valid"
                valid_count += 1
                logger.info(f"Row {jrow.row_number} validated successfully")
            except Exception as e:
                logger.error(f"VALIDATION FAILED for row {jrow.row_number}: {type(e).__name__}: {str(e)}")
                logger.error(f"Mapped data: {mapped_data}")
                jrow.status = "error"
                jrow.error = f"{type(e).__name__}: {str(e)}"
            await jrow.save()

        job.meta["valid_count"] = valid_count
        job.status = "importing"

        await manager.broadcast(
            {
                "status": job.status,
                "progress": await get_progress(),
                "meta": job.meta,
            },
            job.id,
        )
        await job.save()

        # === 3. Import to QBO ===
        user = await job.user
        client = await get_qbo_client(user)
        success = 0

        valid_rows = await JobRow.filter(job=job, status="valid").all()
        logger.info(f"Starting import of {len(valid_rows)} valid rows for job {job_id}")

        for row in valid_rows:
            payload_to_send = row.payload

            logger.info(f"Attempting to create {object_type} for row {row.row_number} with payload: {payload_to_send}")

            try:
                # FIXED: Remove the leading /v3/company/{realm_id} — it's already in the client's base_url
                resp = await client.post(
                    f"/{object_type.lower()}?minorversion=75",
                    json=payload_to_send
                )

                logger.info(f"QBO Response Status for row {row.row_number}: {resp.status_code}")
                logger.info(f"QBO Response Body for row {row.row_number}: {resp.text}")

                if resp.status_code in (200, 201):
                    try:
                        response_json = resp.json()
                        qbo_obj = response_json.get(object_type.capitalize(), {}) or response_json.get("Customer", {})
                        qbo_id = qbo_obj.get("Id")
                        sync_token = qbo_obj.get("SyncToken", 0)

                        row.status = "success"
                        row.meta = {"qbo_id": qbo_id, "sync_token": sync_token}
                        success += 1
                        logger.info(f"Successfully created {object_type} - QBO ID: {qbo_id}")
                    except Exception as json_e:
                        logger.error(f"Failed to parse success JSON for row {row.row_number}: {json_e}")
                        row.status = "error"
                        row.error = f"JSON parse error: {str(json_e)}"
                else:
                    error_detail = resp.text or "Empty response body"
                    logger.error(f"QBO API ERROR for row {row.row_number} - Status {resp.status_code}: {error_detail}")
                    row.status = "error"
                    row.error = f"QBO {resp.status_code}: {error_detail[:500]}"

            except Exception as e:
                logger.exception(f"Network/exception error importing row {row.row_number}")
                row.status = "error"
                row.error = f"Request failed: {str(e)}"

            await row.save()

        # === Final status ===
        total_valid = len(valid_rows)
        # FIXED: Use shorter status to avoid truncation (until you expand the DB column)
        job.status = "completed" if success == total_valid else "partial_success"

        job.meta.update({"success_count": success, "valid_count": total_valid})

        await manager.broadcast(
            {
                "status": job.status,
                "progress": await get_progress(),
                "meta": job.meta,
            },
            job.id,
        )
        await job.save()

        await client.aclose()
        await Tortoise.close_connections()

    asyncio.run(run())