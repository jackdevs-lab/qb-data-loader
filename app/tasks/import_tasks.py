# app/tasks/import_tasks.py

import asyncio
import os
import io
import csv
import json
import logging
from typing import Dict, Any, List
import nest_asyncio
from urllib.parse import urlparse
import datetime
nest_asyncio.apply()

from celery import Celery
from tortoise import Tortoise
from app.core.websocket import manager
from app.models.db import Job, JobRow
from app.core.db import TORTOISE_ORM
from app.core.qbo import get_qbo_client
from app.schemas.customer import CustomerCanonical, EmailAddress, PhoneNumber, PhysicalAddress, Website
from app.schemas.validation import RowValidationResult
logger = logging.getLogger(__name__)

celery_app = Celery(
    "qb_loader",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)

celery_app.conf.update(
    task_track_started=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)


def normalize_to_canonical(raw_mapping: Dict[str, str], row_data: Dict[str, str]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    boolean_fields = {"Taxable", "Active", "Job", "BillWithParent", "IsProject"}
    ref_fields = {"ParentRef", "CurrencyRef", "DefaultTaxCodeRef", "SalesTermRef", "PaymentMethodRef", "ARAccountRef"}
    phone_fields = [
        "PrimaryPhone.FreeFormNumber",
        "Mobile.FreeFormNumber",
        "Fax.FreeFormNumber",
        "AlternatePhone.FreeFormNumber",
    ]
    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"]

    for csv_header, qbo_path in raw_mapping.items():
        value = row_data.get(csv_header, "").strip()
        if not value:
            continue

        if qbo_path == "DisplayName":
            if len(value) > 500 or any(c in value for c in ":\t\n"):
                raise ValueError(f"Invalid DisplayName: max 500 chars, no :, \\t, \\n")
            data["DisplayName"] = value

        elif qbo_path == "PrimaryEmailAddr.Address":
            if '@' not in value or '.' not in value:
                raise ValueError("Invalid email format: must contain @ and .")
            # FIXED: Use the correct field name "Address"
            data["PrimaryEmailAddr"] = EmailAddress(Address=value)

        elif qbo_path in phone_fields:
            if len(value) > 30:
                raise ValueError(f"Phone number too long: max 30 chars")
            field_name = qbo_path.split(".")[0]
            data[field_name] = PhoneNumber(FreeFormNumber=value)

        elif qbo_path == "WebAddr.URI":
            parsed = urlparse(value)
            if not (parsed.scheme in ('http', 'https') and parsed.netloc):
                raise ValueError("Invalid website URL: must start with http:// or https:// and have a valid domain")
            if len(value) > 1000:
                raise ValueError("Website URL too long: max 1000 chars")
            data["WebAddr"] = Website(URI=value)

        elif qbo_path.startswith("BillAddr.") or qbo_path.startswith("ShipAddr."):
            addr_type, field = qbo_path.split(".", 1)
            if addr_type not in data:
                data[addr_type] = {}
            data[addr_type][field] = value

        elif qbo_path in boolean_fields:
            data[qbo_path] = value.lower() in ("true", "1", "yes", "y", "on", "t")

        elif qbo_path in {
            "CompanyName", "Title", "GivenName", "MiddleName", "FamilyName",
            "Suffix", "Notes", "PreferredDeliveryMethod", "ResaleNum",
            "PrimaryTaxIdentifier", "SecondaryTaxIdentifier", "BusinessNumber",
            "GSTIN", "GSTRegistrationType", "Source", "PrintOnCheckName",
            "TaxExemptionReasonId"
        }:
            if qbo_path == "Title" and len(value) > 16:
                raise ValueError("Title too long: max 16 chars")
            elif qbo_path in {"GivenName", "MiddleName", "FamilyName", "CompanyName", "PrintOnCheckName"} and len(value) > 100:
                raise ValueError(f"{qbo_path} too long: max 100 chars")
            elif qbo_path == "Suffix" and len(value) > 16:
                raise ValueError("Suffix too long: max 16 chars")
            elif qbo_path == "Notes" and len(value) > 2000:
                raise ValueError("Notes too long: max 2000 chars")
            elif qbo_path in {"ResaleNum", "GSTIN"} and len(value) > 16:
                raise ValueError(f"{qbo_path} too long: max 16 chars")
            elif qbo_path == "BusinessNumber" and len(value) > 10:
                raise ValueError("BusinessNumber too long: max 10 chars")
            elif qbo_path == "PreferredDeliveryMethod" and value not in ("Print", "Email", "None"):
                raise ValueError("Invalid PreferredDeliveryMethod: must be Print, Email, or None")
            data[qbo_path] = value

        elif qbo_path == "Balance":
            try:
                data["Balance"] = float(value)
            except ValueError:
                raise ValueError("Invalid Balance: must be a number")

        elif qbo_path == "OpenBalanceDate":
            for fmt in date_formats:
                try:
                    dt = datetime.datetime.strptime(value, fmt)
                    data["OpenBalanceDate"] = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    pass
            else:
                raise ValueError(f"Invalid OpenBalanceDate: unsupported format (tried {', '.join(date_formats)})")

        elif qbo_path in ref_fields:
            data[qbo_path] = {"value": value}

        else:
            data[qbo_path] = value

    # Convert address dicts to models, remove if empty
    for addr_type in ("BillAddr", "ShipAddr"):
        if addr_type in data and isinstance(data[addr_type], dict):
            addr_dict = data[addr_type]
            if any(v.strip() for v in addr_dict.values() if isinstance(v, str)):
                data[addr_type] = PhysicalAddress(**addr_dict)
            else:
                del data[addr_type]

    return data

@celery_app.task(bind=True, max_retries=5, default_retry_delay=30)
def import_valid_rows_task(self, job_id: int, csv_content: str, object_type: str):
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

        user = job.user
        client = await get_qbo_client(user)
        realm_id = user.qbo_realm_id

        # If override_existing, query existing customers by DisplayName
        override_existing = job.meta.get("override_existing", False)
        existing_customers = {}
        if override_existing:
            unique_names = set()
            valid_rows_temp = await JobRow.filter(job=job, status="valid").all()
            for row in valid_rows_temp:
                display_name = row.payload["Customer"].get("DisplayName", "").strip()
                if display_name:
                    unique_names.add(display_name)

            if unique_names:
                # Batch query (limit to 500 names per query; for simplicity, assume <500, else split)
                escaped_names = [name.replace("'", "''") for name in list(unique_names)]
                name_list = "', '".join(escaped_names)
                query = f"SELECT Id, DisplayName, SyncToken FROM Customer WHERE DisplayName IN ('{name_list}')"

                resp = await client.get(
                    "/query",
                    params={"query": query, "minorversion": "75"}
                )

                if resp.status_code == 200:
                    data = resp.json()
                    for cust in data.get("QueryResponse", {}).get("Customer", []):
                        name = cust.get("DisplayName", "").strip()
                        if name:
                            existing_customers[name] = {
                                "Id": cust.get("Id"),
                                "SyncToken": cust.get("SyncToken")
                            }
                else:
                    logger.warning(f"QBO duplicate query failed: {resp.status_code} {resp.text}")
                    # Proceed without overrides if query fails

        success_count = 0
        valid_rows = await JobRow.filter(job=job, status="valid").all()

        BATCH_SIZE = 30
        for i in range(0, len(valid_rows), BATCH_SIZE):
            batch = valid_rows[i:i + BATCH_SIZE]
            batch_requests = []
            for row in batch:
                payload = row.payload["Customer"]
                display_name = payload.get("DisplayName", "").strip()
                operation = "create"
                if override_existing and display_name in existing_customers:
                    existing = existing_customers[display_name]
                    payload["Id"] = existing["Id"]
                    payload["SyncToken"] = existing["SyncToken"]
                    payload["sparse"] = True  # Sparse update
                    operation = "update"

                batch_requests.append({
                    "bId": str(row.id),
                    "operation": operation,
                    "Customer": payload
                })

            batch_body = {"BatchItemRequest": batch_requests}

            try:
                resp = await client.post(
                    "/batch",
                    json=batch_body,
                    params={"minorversion": "75"}
                )

                if resp.status_code in (200, 201):
                    try:
                        response_data = resp.json()
                    except Exception as e:
                        error_msg = f"Invalid JSON response: {str(e)}"
                        for jrow in batch:
                            jrow.status = "error"
                            jrow.error = error_msg
                            await jrow.save()
                        continue

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
                            errors = fault.get("Error", [])
                            if errors:
                                err = errors[0]
                                code = err.get("code", "Unknown")
                                message = err.get("Message", "Unknown error")
                                detail = err.get("Detail", "")
                                jrow.error = f"QBO Error {code}: {message} — {detail}"
                            else:
                                jrow.error = "Unknown QBO fault"
                            jrow.status = "error"

                        await jrow.save()

                else:
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

        job.status = "completed" if success_count == len(valid_rows) else "partial_success"
        job.meta.update({
            "success_count": success_count,
            "failed_count": len(valid_rows) - success_count
        })
        await job.save()
        await broadcast_progress()

        await client.aclose()
        await Tortoise.close_connections()

    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        loop.run_until_complete(run_import())
    except Exception as exc:
        logger.exception("Import task failed catastrophically")
        raise self.retry(exc=exc)