# app/tasks/import_tasks.py
import os
import asyncio
import io
import csv
from celery import Celery
from tortoise import Tortoise
import logging
from typing import Dict, Any

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


def normalize_mapping_for_customer(
    raw_mapping: Dict[str, str],
    row_data: Dict[str, str]
) -> Dict[str, Any]:
    """
    Converts frontend mapping {CSV column: QBO dotted path}
    into a dict with keys matching the CustomerRow Pydantic model fields.
    """
    normalized: Dict[str, Any] = {}

    for csv_header, qbo_path in raw_mapping.items():
        if csv_header not in row_data:
            continue

        value = row_data[csv_header].strip()
        if not value:  # Skip empty values
            continue

        # Direct required field
        if qbo_path == "DisplayName":
            normalized["DisplayName"] = value

        # Email
        elif qbo_path == "PrimaryEmailAddr.Address":
            normalized["PrimaryEmailAddr"] = value

        # Primary Phone
        elif qbo_path == "PrimaryPhone.FreeFormNumber":
            normalized["PrimaryPhone"] = value

        # Optional phone fields (add to model later if needed)
        elif qbo_path == "Mobile.FreeFormNumber":
            normalized["Mobile"] = value

        elif qbo_path == "Fax.FreeFormNumber":
            normalized["Fax"] = value

        elif qbo_path == "AlternatePhone.FreeFormNumber":
            normalized["AlternatePhone"] = value

        # Website
        elif qbo_path == "WebAddr.URI":
            normalized["WebAddr"] = value

        # Billing Address fields
        elif qbo_path.startswith("BillAddr."):
            field_part = qbo_path[len("BillAddr."):]  # e.g. "City", "Line1"
            internal_key = f"BillAddr_{field_part}"
            normalized[internal_key] = value

        # You can extend this for ShipAddr, Notes, Taxable, etc. as needed

    return normalized


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

        mapping = job.meta.get("mapping", {})

        # Optional: load from saved template (existing logic preserved)
        mapping_id = job.meta.get("mapping_id")
        if mapping_id and not mapping:
            template = await MappingTemplate.get_or_none(id=mapping_id, user=await job.user)
            if template and template.object_type.lower() == object_type.lower():
                mapping = template.mapping

        valid_count = 0
        current_rows = await JobRow.filter(job=job).all()

        validator_fields = set(validator_class.model_fields.keys())

        for jrow in current_rows:
            raw = jrow.raw_data

            # === NEW: Correct mapping using normalizer ===
            data_for_model = normalize_mapping_for_customer(mapping, raw)

            # Fallback: exact CSV column name matches model field
            for col, val in raw.items():
                cleaned = val.strip()
                if cleaned and col in validator_fields and col not in data_for_model:
                    data_for_model[col] = cleaned

            logger.info(f"Mapped data: {data_for_model}")

            try:
                validated = validator_class(**data_for_model)
                jrow.payload = validated.model_dump(exclude_unset=True, exclude_none=True)
                jrow.status = "valid"
                valid_count += 1
                logger.info(f"Row {jrow.row_number} validated successfully")
            except Exception as e:
                logger.error(f"VALIDATION FAILED for row {jrow.row_number}: {e}")
                logger.error(f"Mapped data was: {data_for_model}")
                jrow.status = "error"
                jrow.error = str(e)
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