# app/api/imports.py

import csv
import io
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Body
from typing import Any, Dict, List
from app.models.db import Job, User
from app.schemas.customer import CustomerCanonical
from app.schemas.validation import (
    DryRunSummary,
    ValidationIssue,
    RowValidationResult,
)
from app.core.auth import get_current_user
from app.core.qbo import get_qbo_client
from app.tasks.import_tasks import normalize_to_canonical

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/import", tags=["import"])


@router.post("/{object_type}")
async def upload_csv_for_mapping(
    object_type: str,
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    if object_type != "customer":
        raise HTTPException(400, "Only customer imports are supported at this time.")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "Invalid file encoding — please save as UTF-8 (with or without BOM).")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames
    if not headers:
        raise HTTPException(400, "CSV has no headers or is empty.")

    rows = list(reader)
    if not rows:
        raise HTTPException(400, "CSV contains no data rows.")

    preview_rows = rows[:50]

    job = await Job.create(
        user=user,
        object_type=object_type,
        status="uploaded",
        meta={
            "filename": file.filename,
            "headers": headers,
            "row_count": len(rows) + 1,  # +1 for header
            "csv_content": text,
            "preview_rows": [dict(r) for r in preview_rows],
        }
    )

    return {
        "job_id": job.id,
        "headers": headers,
        "preview_rows": [dict(r) for r in preview_rows],
        "valid": True
    }


@router.post("/{object_type}/{job_id}/start")
async def start_import_with_mapping(
    object_type: str,
    job_id: int,
    payload: dict = Body(...),
    user: User = Depends(get_current_user),
):
    mapping = payload.get("mapping")
    override_existing = payload.get("override_existing", False)
    edited_rows = payload.get("rows")  # ← Final edited data from preview

    job = await Job.get_or_none(id=job_id, user=user)
    if not job:
        raise HTTPException(404, "Job not found or access denied.")
    if job.status not in ["uploaded", "dry_run_complete", "dry_run_failed"]:
        raise HTTPException(400, "Job is already processing or completed.")

    if not mapping or "DisplayName" not in mapping.values():
        raise HTTPException(400, "DisplayName is required — please map a column to it.")

    # Save mapping and override
    job.meta["mapping"] = mapping
    job.meta["override_existing"] = override_existing

    # === If user made edits, overwrite csv_content with fixed version ===
    if edited_rows and isinstance(edited_rows, list) and len(edited_rows) > 0:
        headers = job.meta.get("headers")
        if not headers:
            raise HTTPException(500, "Headers missing from job metadata.")

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()

        for row_dict in edited_rows:
            clean_row = {}
            for h in headers:
                value = row_dict.get(h, "")
                clean_row[h] = str(value).strip() if value is not None else ""
            writer.writerow(clean_row)

        # Overwrite the original CSV content with the edited one
        job.meta["csv_content"] = output.getvalue()
        job.meta["edited_rows_used"] = True  # Optional flag for debugging/history

    job.status = "queued"
    await job.save(update_fields=["meta", "status"])

    # Trigger Celery task — now uses the edited csv_content if edits were made
    from app.tasks.import_tasks import import_valid_rows_task
    import_valid_rows_task.delay(
        job_id=job.id,
        csv_content=job.meta["csv_content"],
        object_type=object_type
    )

    return {"message": "Import queued successfully!"}


@router.post("/customer/{job_id}/dry-run")
async def dry_run_customer_import(
    job_id: int,
    payload: Dict[str, Any] = Body(...),  # Full payload: mapping + optional rows
    user: User = Depends(get_current_user),
):
    mapping = payload.get("mapping")
    edited_rows = payload.get("rows")  # Optional: list of dicts from frontend edits

    if not mapping or "DisplayName" not in mapping.values():
        raise HTTPException(400, "DisplayName is required in mapping.")

    job = await Job.get_or_none(id=job_id, user=user)
    if not job:
        raise HTTPException(404, "Job not found or access denied.")
    if job.object_type != "customer":
        raise HTTPException(400, "Dry run only supported for customer imports.")

    # Always use stored headers (saved on upload)
    csv_headers = job.meta.get("headers")
    if not csv_headers:
        raise HTTPException(500, "CSV headers missing from job metadata.")

    # === Determine which rows to validate ===
    if edited_rows and isinstance(edited_rows, list) and len(edited_rows) > 0:
        # Use edited rows from frontend — reconstruct with correct headers/order
        rows = []
        for row_dict in edited_rows:
            row = {}
            for header in csv_headers:
                # Get value safely, convert to string, strip
                value = row_dict.get(header, "")
                row[header] = str(value).strip() if value is not None else ""
            rows.append(row)
    else:
        # Fallback: original uploaded CSV
        csv_content = job.meta.get("csv_content")
        if not csv_content:
            raise HTTPException(500, "CSV content missing from job metadata.")
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)

    # === Proceed with validation (unchanged from your excellent code) ===
    row_results: List[RowValidationResult] = []
    display_name_to_rows: Dict[str, List[int]] = {}

    for idx, row in enumerate(rows, start=2):  # Row 2 = first data row
        status = "error"
        issues: List[ValidationIssue] = []
        qbo_payload = None

        try:
            mapped_data = normalize_to_canonical(mapping, row)
            customer = CustomerCanonical(**mapped_data)
            qbo_payload = customer.to_qbo_payload()
            status = "valid"

            # Track for duplicate detection
            display_name = customer.DisplayName.strip()
            display_name_to_rows.setdefault(display_name, []).append(idx)

        except Exception as e:
            from pydantic import ValidationError

            if isinstance(e, ValidationError):
                for err in e.errors():
                    field_path = ".".join(str(loc) for loc in err["loc"] if loc != "__root__")
                    issues.append(ValidationIssue(
                        level="error",
                        code=err["type"],
                        message=err["msg"],
                        field=field_path or "General",
                        row=idx,
                    ))
            else:
                issues.append(ValidationIssue(
                    level="error",
                    code="unexpected_error",
                    message=str(e),
                    field=None,
                    row=idx,
                ))

        row_results.append(RowValidationResult(
            row_number=idx,
            status=status,
            issues=issues,
            normalized_data=qbo_payload,
        ))

    # === Semantic validation: local duplicates ===
    semantic_issues: List[ValidationIssue] = []

    for display_name, row_nums in display_name_to_rows.items():
        if len(row_nums) > 1:
            for rn in row_nums:
                semantic_issues.append(ValidationIssue(
                    level="error",
                    code="local_duplicate_displayname",
                    message=f"Duplicate DisplayName '{display_name}' found in CSV (also in row(s): {', '.join(map(str, row_nums))})",
                    field="DisplayName",
                    row=rn,
                ))

    # === Check duplicates in QuickBooks ===
    unique_names = list(display_name_to_rows.keys())
    if unique_names:
        try:
            client = await get_qbo_client(user)

            escaped_names = [name.replace("'", "''") for name in unique_names[:500]]
            name_list = "', '".join(escaped_names)
            query = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName IN ('{name_list}')"

            resp = await client.get(
                "/query",
                params={"query": query, "minorversion": "75"}
            )

            if resp.status_code == 200:
                data = resp.json()
                existing_customers = {
                    cust["DisplayName"].strip(): cust["Id"]
                    for cust in data.get("QueryResponse", {}).get("Customer", [])
                }

                for display_name in unique_names:
                    if display_name in existing_customers:
                        for rn in display_name_to_rows[display_name]:
                            semantic_issues.append(ValidationIssue(
                                level="error",
                                code="qbo_duplicate_displayname",
                                message=f"Customer '{display_name}' already exists in QuickBooks (Id: {existing_customers[display_name]})",
                                field="DisplayName",
                                row=rn,
                            ))
            else:
                logger.warning(f"QBO duplicate check failed: {resp.status_code} {resp.text}")
                semantic_issues.append(ValidationIssue(
                    level="warning",
                    code="qbo_check_failed",
                    message="Could not verify existing customers in QuickBooks.",
                    row=None,
                ))

            await client.aclose()

        except Exception as e:
            logger.error(f"QBO connection error during duplicate check: {e}")
            semantic_issues.append(ValidationIssue(
                level="warning",
                code="qbo_connection_error",
                message="Could not connect to QuickBooks to check for duplicates.",
                row=None,
            ))

    # === Merge semantic issues ===
    for issue in semantic_issues:
        if issue.row:
            for result in row_results:
                if result.row_number == issue.row:
                    result.issues.append(issue)
                    if issue.level == "error":
                        result.status = "error"

    # === Build summary ===
    total = len(rows)
    will_succeed = sum(1 for r in row_results if r.status == "valid")
    will_fail = total - will_succeed
    warnings = sum(1 for i in semantic_issues if i.level == "warning")

    all_issues = [issue for r in row_results for issue in r.issues] + [
        i for i in semantic_issues if i.row is None
    ]

    summary = DryRunSummary(
        total_rows=total,
        will_succeed=will_succeed,
        will_fail=will_fail,
        warnings=warnings,
        issues=all_issues,
    )

    # Save last dry-run result
    job.meta["last_dry_run"] = {
        "summary": summary.dict(),
        "mapping_used": mapping,
        "row_details": [r.dict() for r in row_results],
    }
    job.status = "dry_run_complete" if will_fail == 0 else "dry_run_failed"
    await job.save()

    return {
        "summary": summary.dict(),
        "rows": [r.dict() for r in row_results],
        "message": "Dry run completed successfully."
    }