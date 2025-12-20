# app/tasks/import_tasks.py

import os
import io
import csv
import json
import logging
from typing import Dict, Any, List

from celery import Celery
from tortoise import Tortoise
from app.core.websocket import manager
from app.models.db import Job, JobRow
from app.core.db import TORTOISE_ORM
from app.core.qbo import get_qbo_client
from app.schemas.customer import CustomerCanonical
from app.schemas.validation import RowValidationResult
from app.schemas.customer import Email, Phone, WebAddr, Address
logger = logging.getLogger(__name__)

celery_app = Celery(
    "qb_loader",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

celery_app.conf.update(
    task_track_started=True,
    worker_prefetch_multiplier=1,  # Better for long-running tasks
    task_acks_late=True,
)


def normalize_to_canonical(raw_mapping: Dict[str, str], row_data: Dict[str, str]) -> Dict[str, Any]:
    """
    Maps CSV row + mapping → dict compatible with CustomerCanonical model.
    All nested fields are converted to proper Pydantic model instances
    so that .model_dump() works correctly in to_qbo_payload().
    """
    data: Dict[str, Any] = {}

    for csv_header, qbo_path in raw_mapping.items():
        value = row_data.get(csv_header, "").strip()
        if not value:
            continue  # Skip empty values entirely

        if qbo_path == "DisplayName":
            data["DisplayName"] = value

        elif qbo_path == "PrimaryEmailAddr.Address":
            data["PrimaryEmailAddr"] = Email(Address=value)

        elif qbo_path in [
            "PrimaryPhone.FreeFormNumber",
            "Mobile.FreeFormNumber",
            "Fax.FreeFormNumber",
            "AlternatePhone.FreeFormNumber",
        ]:
            field_name = qbo_path.split(".")[0]
            data[field_name] = Phone(FreeFormNumber=value)

        elif qbo_path == "WebAddr.URI":
            data["WebAddr"] = WebAddr(URI=value)

        elif qbo_path == "PrintOnCheckName":
            data["PrintOnCheckName"] = value

        elif qbo_path.startswith("BillAddr.") or qbo_path.startswith("ShipAddr."):
            addr_type, field = qbo_path.split(".", 1)
            if addr_type not in data:
                data[addr_type] = {}
            data[addr_type][field] = value

        elif qbo_path in {"Taxable", "Active", "Job", "BillWithParent"}:
            # Boolean fields
            data[qbo_path] = value.lower() in ("true", "1", "yes", "y", "on", "t")

        elif qbo_path in {
            "CompanyName", "Title", "GivenName", "MiddleName", "FamilyName",
            "Suffix", "Notes", "ParentRef", "CurrencyRef"
        }:
            # Direct scalar string or dict fields
            data[qbo_path] = value

        else:
            # Fallback: any other unmapped but valid field (e.g., future extensions)
            data[qbo_path] = value

    # === Convert collected address dicts into proper Address model instances ===
    for addr_type in ("BillAddr", "ShipAddr"):
        if addr_type in data and isinstance(data[addr_type], dict):
            addr_dict = data[addr_type]
            # Only create Address object if there's meaningful data
            if any(v.strip() for v in addr_dict.values() if isinstance(v, str)):
                data[addr_type] = Address(**addr_dict)
            else:
                # Remove empty address entirely
                del data[addr_type]

    return data

@celery_app.task(bind=True, max_retries=5, default_retry_delay=30)
def import_valid_rows_task(self, job_id: int, csv_content: str, object_type: str):
    """
    Main Celery task: parse → validate → import customers.
    Now properly async-compatible using run_in_executor or async worker.
    """
    async def run_import():
        await Tortoise.init(config=TORTOISE_ORM)
        job = await Job.get(id=job_id).prefetch_related("user")

        async def broadcast_progress():
            progress = {
                "total": await JobRow.filter(job=job).count(),
                "valid": await JobRow.filter(job=job, status="valid").count(),
                "error": await JobRow.filter(job=job, status="error").count(),
                "success": await JobRow.filter(job=job, status="success").count(),
                "processing": await JobRow.filter(job=job, status="processing").count(),
            }
            await manager.broadcast({
                "status": job.status,
                "progress": progress,
                "meta": job.meta
            }, job.id)

        # === Parsing & Setup ===
        job.status = "parsing"
        await job.save()
        await broadcast_progress()

        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)

        if not rows:
            job.status = "error"
            job.meta["error"] = "CSV contains no data rows"
            await job.save()
            await broadcast_progress()
            return

        job_rows = [
            JobRow(job=job, row_number=idx + 2, raw_data=row, status="pending")
            for idx, row in enumerate(rows)
        ]
        await JobRow.bulk_create(job_rows, batch_size=100)

        job.meta["total_rows"] = len(rows)
        job.status = "validating"
        await job.save()
        await broadcast_progress()

        # === Validation ===
        mapping = job.meta.get("mapping", {})
        if not mapping:
            job.status = "error"
            job.meta["error"] = "No mapping found"
            await job.save()
            return

        valid_count = 0
        batch_update: List[JobRow] = []

        for jrow in await JobRow.filter(job=job).all():
            try:
                model_input = normalize_to_canonical(mapping, jrow.raw_data)
                customer = CustomerCanonical(**model_input)
                jrow.payload = customer.to_qbo_payload()
                jrow.status = "valid"
                valid_count += 1
            except Exception as e:
                logger.error(f"Validation failed for row {jrow.row_number}: {e}")
                jrow.status = "error"
                jrow.error = str(e)

            batch_update.append(jrow)

            if len(batch_update) >= 50:
                await JobRow.bulk_update(batch_update, fields=["payload", "status", "error"])
                batch_update.clear()
                await broadcast_progress()

        if batch_update:
            await JobRow.bulk_update(batch_update, fields=["payload", "status", "error"])

        job.meta["valid_count"] = valid_count
        job.status = "importing"
        await job.save()
        await broadcast_progress()

        # === Import to QBO ===
        user = job.user
        client = await get_qbo_client(user)
        realm_id = user.qbo_realm_id  # ← Assumes you store this on User model

        success_count = 0
        valid_rows = await JobRow.filter(job=job, status="valid").all()

        # Optional: Batch create (up to 30 per batch — QBO limit)
        BATCH_SIZE = 30
        for i in range(0, len(valid_rows), BATCH_SIZE):
            batch = valid_rows[i:i + BATCH_SIZE]
            batch_payloads = [row.payload["Customer"] for row in batch]  # Extract inner Customer
            batch_body = {"BatchItemRequest": [
                {"bId": str(row.id), "Customer": payload}
                for row, payload in zip(batch, batch_payloads)
            ]}

            try:
                resp = await client.post(
                    "/batch",  # ← Just "/batch"
                    json=batch_body,
                    params={"minorversion": "75"}
                )

                if resp.status_code in (200, 201):
                    response_data = resp.json()
                    batch_responses = response_data.get("BatchItemResponse", [])

                    for item_resp in batch_responses:
                        b_id = int(item_resp["bId"])
                        jrow = next(r for r in batch if r.id == b_id)

                        if "Customer" in item_resp:
                            cust = item_resp["Customer"]
                            jrow.status = "success"
                            jrow.meta = {"qbo_id": cust.get("Id"), "sync_token": cust.get("SyncToken", "0")}
                            success_count += 1
                        else:
                            fault = item_resp.get("Fault", {})
                            errors = fault.get("Error", [{}])[0]
                            code = errors.get("code", "Unknown")
                            message = errors.get("Message", "Unknown error")
                            detail = errors.get("Detail", "")
                            jrow.status = "error"
                            jrow.error = f"QBO Error {code}: {message} — {detail}"

                        await jrow.save()

                else:
                    # Fallback: mark all in batch as error
                    error_msg = resp.text[:500]
                    for jrow in batch:
                        jrow.status = "error"
                        jrow.error = f"Batch failed ({resp.status_code}): {error_msg}"
                        await jrow.save()

            except Exception as e:
                logger.error(f"Batch import failed: {e}")
                for jrow in batch:
                    jrow.status = "error"
                    jrow.error = f"Request failed: {str(e)}"
                    await jrow.save()

            await broadcast_progress()

        # === Finalize ===
        job.status = "completed" if success_count == len(valid_rows) else "partial_success"
        job.meta.update({
            "success_count": success_count,
            "failed_count": len(valid_rows) - success_count
        })
        await job.save()
        await broadcast_progress()

        await client.aclose()
        await Tortoise.close_connections()

    # Run the async function properly
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_import())
    except Exception as exc:
        logger.exception("Import task failed catastrophically")
        raise self.retry(exc=exc)