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
from app.core.qbo import get_qbo_client
from app.schemas.customer import CustomerCanonical, WebAddr, Phone, Email, Address
logger = logging.getLogger(__name__)

celery_app = Celery(
    "qb_loader",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

celery_app.conf.update(task_track_started=True)


def normalize_to_canonical(raw_mapping: Dict[str, str], row_data: Dict[str, str]) -> Dict[str, Any]:
    """
    Maps CSV columns → properly structured data with model instances
    expected by CustomerCanonical. Super robust and validator-friendly.
    """
    data: Dict[str, Any] = {}

    for csv_header, qbo_path in raw_mapping.items():
        raw_value = row_data.get(csv_header, "").strip()
        if not raw_value:
            continue  # Skip empty values — let Pydantic defaults handle None

        # === Scalar fields (direct assignment) ===
        scalar_fields = {
            "DisplayName",
            "CompanyName",
            "Title",
            "GivenName",
            "MiddleName",
            "FamilyName",
            "Suffix",
            "Notes",
            "Taxable",
            "Active",
            "Job",
            "BillWithParent",
            # Add CurrencyRef.value → CurrencyRef later if needed
        }

        if qbo_path in scalar_fields:
            if qbo_path in {"Taxable", "Active", "Job", "BillWithParent"}:
                data[qbo_path] = raw_value.lower() in ("true", "1", "yes", "y", "on")
            else:
                data[qbo_path] = raw_value
            continue

        # === Nested model fields ===
        if qbo_path == "PrimaryEmailAddr.Address":
            data["PrimaryEmailAddr"] = Email(Address=raw_value)
            continue

        if qbo_path == "WebAddr.URI":
            data["WebAddr"] = WebAddr(URI=raw_value)  # Runs URL cleaning + validation early
            continue

        if qbo_path in {
            "PrimaryPhone.FreeFormNumber",
            "Mobile.FreeFormNumber",
            "Fax.FreeFormNumber",
            "AlternatePhone.FreeFormNumber",
        }:
            field_name = qbo_path.split(".")[0]
            data[field_name] = Phone(FreeFormNumber=raw_value)  # Runs phone cleaning
            continue

        if qbo_path.startswith(("BillAddr.", "ShipAddr.")):
            addr_type, field = qbo_path.split(".", 1)
            if addr_type not in data:
                data[addr_type] = {}
            data[addr_type][field] = raw_value
            continue

        # === Fallback: direct scalar (for future-proofing new fields) ===
        data[qbo_path] = raw_value

    # === Final step: Convert partial address dicts to Address model instances ===
    for addr_key in ("BillAddr", "ShipAddr"):
        if addr_key in data and isinstance(data[addr_key], dict):
            try:
                data[addr_key] = Address(**data[addr_key])  # Validates + cleans all fields
            except Exception as e:
                # In real use, you could log this, but since validation happens later anyway,
                # just leave as dict — Pydantic will catch and report properly
                pass

    return data


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

        # === Parsing ===
        job.status = "parsing"
        await manager.broadcast({"status": job.status, "progress": await get_progress(), "meta": job.meta}, job.id)
        await job.save()

        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)

        job_rows = [
            JobRow(job=job, row_number=idx + 2, raw_data=row, status="pending")
            for idx, row in enumerate(rows)
        ]
        await JobRow.bulk_create(job_rows)

        job.meta["row_count"] = len(rows)
        job.status = "validating"
        await manager.broadcast({"status": job.status, "progress": await get_progress(), "meta": job.meta}, job.id)
        await job.save()

        # === Validation using NEW canonical schema ===
        if object_type != "customer":
            job.status = "error"
            job.meta["error"] = f"Unsupported object_type: {object_type}"
            await job.save()
            await manager.broadcast({"status": job.status, "meta": job.meta}, job.id)
            return

        mapping = job.meta.get("mapping", {})

        # Load saved mapping template if needed
        mapping_id = job.meta.get("mapping_id")
        if mapping_id and not mapping:
            template = await MappingTemplate.get_or_none(id=mapping_id, user=await job.user)
            if template and template.object_type.lower() == "customer":
                mapping = template.mapping

        valid_count = 0
        current_rows = await JobRow.filter(job=job).all()

        for jrow in current_rows:
            raw = jrow.raw_data
            data_for_model = normalize_to_canonical(mapping, raw)

            try:
                # This triggers full Pydantic validation + cleaning
                customer = CustomerCanonical(**data_for_model)

                # Use our custom method to get QBO-ready payload
                jrow.payload = customer.to_qbo_payload()
                jrow.status = "valid"
                valid_count += 1
                logger.info(f"Row {jrow.row_number} validated successfully")

            except Exception as e:
                logger.error(f"Validation failed for row {jrow.row_number}: {e}")
                logger.error(f"Data: {data_for_model}")
                jrow.status = "error"
                jrow.error = str(e)

            await jrow.save()

        job.meta["valid_count"] = valid_count
        job.status = "importing"
        await manager.broadcast({"status": job.status, "progress": await get_progress(), "meta": job.meta}, job.id)
        await job.save()

        # === Import valid rows to QBO ===
        user = await job.user
        client = await get_qbo_client(user)
        success = 0
        valid_rows = await JobRow.filter(job=job, status="valid").all()

        for row in valid_rows:
            payload = row.payload

            try:
                resp = await client.post(f"/customer?minorversion=75", json=payload)

                if resp.status_code in (200, 201):
                    resp_json = resp.json()
                    customer_obj = resp_json.get("Customer", {})
                    qbo_id = customer_obj.get("Id")
                    sync_token = customer_obj.get("SyncToken", 0)

                    row.status = "success"
                    row.meta = {"qbo_id": qbo_id, "sync_token": sync_token}
                    success += 1
                else:
                    try:
                        fault_data = resp.json()
                        fault = fault_data.get("Fault", {})
                        errors = fault.get("Error", [])
                        if errors:
                            err = errors[0]
                            message = err.get("Message", "Unknown error")
                            code = err.get("code", "Unknown")
                            detail = err.get("Detail", "")
                            full_error = f"{message} (Code: {code}) — {detail}"
                        else:
                            full_error = resp.text[:1000]
                    except Exception:
                        full_error = resp.text[:1000]

                    row.status = "error"
                    row.error = f"QBO {resp.status_code}: {full_error}"
                    logger.error(f"QBO Create Failed (Row {row.row_number}): {full_error}")
            except Exception as e:
                row.status = "error"
                row.error = f"Request failed: {str(e)}"

            await row.save()

        # === Final ===
        job.status = "completed" if success == len(valid_rows) else "partial_success"
        job.meta.update({"success_count": success})
        await manager.broadcast({"status": job.status, "progress": await get_progress(), "meta": job.meta}, job.id)
        await job.save()

        await client.aclose()
        await Tortoise.close_connections()

    asyncio.run(run())