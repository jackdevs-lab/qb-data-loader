# app/api/imports.py

import csv
import io
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Body
from typing import Dict, List
from app.models.db import Job, User
from app.schemas.customer import CustomerCanonical
from app.schemas.validation import (
    DryRunSummary,
    ValidationIssue,
    RowValidationResult,
)
from app.core.auth import get_current_user
from app.core.qbo import get_qbo_client
from app.tasks.import_tasks import normalize_to_canonical  # Reuse robust normalizer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/import", tags=["import"])


# === Existing upload endpoint (unchanged except minor cleanup) ===
@router.post("/{object_type}")
async def upload_csv_for_mapping(
    object_type: str,
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    if object_type != "customer":
        raise HTTPException(400, "Only customer supported for now")

    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "Invalid file encoding — please save as UTF-8")

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames

    if not headers:
        raise HTTPException(400, "CSV has no headers or is empty")

    rows = list(reader)
    preview_rows = rows[:50]

    job = await Job.create(
        user=user,
        object_type=object_type,
        status="uploaded",
        meta={
            "filename": file.filename,
            "headers": headers,
            "row_count": len(rows) + 1,
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


# === Existing start import (unchanged) ===
@router.post("/{object_type}/{job_id}/start")
async def start_import_with_mapping(
    
    object_type: str,
    job_id: int,
    mapping: dict[str, str] = Body(..., embed=True),  # ← ADD embed=True
    user: User = Depends(get_current_user),
):
    override_existing: bool = Body(False, embed=True)
    job = await Job.get_or_none(id=job_id, user=user)
    if not job:
        raise HTTPException(404, "Job not found or access denied")
    if job.status not in ["uploaded", "dry_run_complete", "dry_run_failed"]:
        raise HTTPException(400, "Job already processing or completed")
    if "DisplayName" not in mapping.values():
        raise HTTPException(400, "DisplayName is required — please map a column to it")

    job.meta["mapping"] = mapping
    job.status = "queued"
    await job.save()

    from app.tasks.import_tasks import import_valid_rows_task
    import_valid_rows_task.delay(job_id=job.id, csv_content=job.meta["csv_content"], object_type=object_type)

    return {"message": "Import started successfully!"}


# === NEW: Dry Run with Full Layered Validation (A + B + C) ===
@router.post("/customer/{job_id}/dry-run")
async def dry_run_customer_import(
    job_id: int,
   mapping: Dict[str, str] = Body(..., embed=True),
    user: User = Depends(get_current_user),
):
    job = await Job.get_or_none(id=job_id, user=user)
    if not job:
        raise HTTPException(404, "Job not found or access denied")

    if job.object_type != "customer":
        raise HTTPException(400, "Dry run only supported for customers")

    if job.status not in ["uploaded", "dry_run_complete", "dry_run_failed"]:
        raise HTTPException(400, "Job must be in uploaded state for dry run")

    if "DisplayName" not in mapping.values():
        raise HTTPException(400, "DisplayName is required")

    csv_content = job.meta.get("csv_content")
    if not csv_content:
        raise HTTPException(500, "CSV content missing from job")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = list(reader)

    row_results: List[RowValidationResult] = []
    issues: List[ValidationIssue] = []

    # === Layer B: Row-level validation ===
    for idx, row in enumerate(rows, start=2):
        mapped_data = normalize_to_canonical(mapping, row)

        try:
            customer = CustomerCanonical(**mapped_data)
            status = "valid"
            row_issues = []
        except Exception as e:
            from pydantic import ValidationError
            status = "error"
            row_issues = []
            if isinstance(e, ValidationError):
                for err in e.errors():
                    field_path = ".".join(str(loc) for loc in err["loc"] if loc)
                    issue = ValidationIssue(
                        level="error",
                        code=err["type"],
                        message=err["msg"],
                        field=field_path or "General",
                        row=idx,
                    )
                    row_issues.append(issue)
                    issues.append(issue)
            else:
                issue = ValidationIssue(
                    level="error",
                    code="validation_error",
                    message=str(e),
                    row=idx,
                )
                row_issues.append(issue)
                issues.append(issue)

        row_results.append(
            RowValidationResult(
                row_number=idx,
                status=status,
                issues=row_issues,
                normalized_data=customer.to_qbo_payload() if status == "valid" else None,
            )
        )

    # === Layer C: Semantic Validation – Local + QBO Duplicates ===
    semantic_issues: List[ValidationIssue] = []

    # Collect DisplayNames from valid rows
    display_names = []
    row_by_displayname: Dict[str, List[int]] = {}

    for result in row_results:
        if result.status == "valid" and result.normalized_data:
            dn = result.normalized_data.get("DisplayName")
            if dn:
                dn_clean = dn.strip()
                if dn_clean:
                    display_names.append(dn_clean)
                    row_by_displayname.setdefault(dn_clean, []).append(result.row_number)

    # Local duplicates in CSV
    local_dupes = {dn: rows for dn, rows in row_by_displayname.items() if len(rows) > 1}
    for dn, row_nums in local_dupes.items():
        for row_num in row_nums:
            semantic_issues.append(ValidationIssue(
                level="error",
                code="local_duplicate_displayname",
                message=f"Duplicate DisplayName in CSV (also in rows: {', '.join(map(str, sorted(set(row_nums))))})",
                field="DisplayName",
                row=row_num,
            ))

    # QBO duplicates
    if display_names:
        try:
            client = await get_qbo_client(user)

            # Build safe IN clause (limit to 1000 for safety)
            unique_names = list(set(display_names))  # Avoid redundant queries
            if unique_names:
                escaped = [name.replace("'", "''") for name in unique_names[:1000]]
                in_clause = "', '".join(escaped)
                query = f"SELECT Id, DisplayName FROM Customer WHERE DisplayName IN ('{in_clause}')"
                
                resp = await client.get(f"/query?query={query}&minorversion=75")
                
                if resp.status_code == 200:
                    data = resp.json()
                    existing = {
                        cust["DisplayName"].strip(): cust["Id"]
                        for cust in data.get("QueryResponse", {}).get("Customer", [])
                    }

                    for dn in unique_names:
                        if dn in existing:
                            affected_rows = row_by_displayname.get(dn, [])
                            for row_num in affected_rows:
                                semantic_issues.append(ValidationIssue(
                                    level="error",
                                    code="qbo_duplicate_displayname",
                                    message=f"Customer '{dn}' already exists in QuickBooks (Id: {existing[dn]})",
                                    field="DisplayName",
                                    row=row_num,
                                ))
                else:
                    semantic_issues.append(ValidationIssue(
                        level="warning",
                        code="qbo_query_failed",
                        message=f"Failed to query QuickBooks for duplicates (status {resp.status_code})",
                    ))

            await client.aclose()

        except Exception as e:
            logger.error(f"QBO duplicate check failed: {e}")
            semantic_issues.append(ValidationIssue(
                level="warning",
                code="qbo_connection_error",
                message="Could not connect to QuickBooks to check for existing customers",
            ))

    # Merge semantic issues
    all_issues = issues + semantic_issues
    for issue in semantic_issues:
        if issue.row and issue.level == "error":
            for r in row_results:
                if r.row_number == issue.row:
                    r.status = "error"
                    r.issues.append(issue)

    # Final summary
    total = len(rows)
    will_succeed = len([r for r in row_results if r.status == "valid"])
    will_fail = total - will_succeed
    warnings = len([i for i in all_issues if i.level == "warning"])

    summary = DryRunSummary(
        total_rows=total,
        will_succeed=will_succeed,
        will_fail=will_fail,
        warnings=warnings,
        issues=all_issues,
    )

    # Save result
    job.meta["last_dry_run"] = {
        "summary": summary.dict(),
        "mapping_used": mapping,
        "performed_at": "now",
    }
    job.status = "dry_run_complete" if will_fail == 0 else "dry_run_failed"
    await job.save()

    return {
        "summary": summary.dict(),
        "rows": [r.dict() for r in row_results],
        "message": "Dry run complete with semantic checks."
    }