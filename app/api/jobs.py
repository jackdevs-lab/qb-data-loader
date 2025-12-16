# app/api/jobs.py
import time
from xmlrpc import client
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from app.models.db import Job, JobRow, User  # Make sure User is imported
from app.core.qbo import get_qbo_client
from app.core.auth import get_current_user  # ← THIS WAS MISSING!
import csv
import io
import httpx

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("/test-qbo-connection")
async def test_qbo(current_user: User = Depends(get_current_user)):
    print(f"DEBUG: Access token exists: {bool(current_user.qbo_access_token)}")
    print(f"DEBUG: Realm ID: {current_user.qbo_realm_id}")
    print(f"DEBUG: Expires at: {current_user.qbo_expires_at}, now: {int(time.time())}, expired: {(current_user.qbo_expires_at or 0) < time.time()}")

    if not current_user.qbo_access_token or not current_user.qbo_realm_id:
        raise HTTPException(400, "Not connected to QuickBooks yet.")

    client = await get_qbo_client(current_user)
    try:
        realm_id = current_user.qbo_realm_id
        resp = await client.get(f"/companyinfo/{realm_id}")
        resp.raise_for_status()
        data = resp.json()
        company = data["CompanyInfo"]
        return {
            "company_name": company["CompanyName"],
            "realm_id": realm_id,
            "connected": True
        }
    except httpx.HTTPStatusError as e:
        # This will now show the actual QBO error message, helpful for debugging
        error_text = e.response.text if e.response else str(e)
        raise HTTPException(status_code=400, detail=f"QuickBooks error: {error_text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")
    finally:
        await client.aclose()


@router.get("")
async def list_jobs(current_user: User = Depends(get_current_user)):
    jobs = await Job.filter(user=current_user).order_by("-created_at").prefetch_related("rows")
    result = []
    for j in jobs:
        total = len(j.rows)
        failed = await j.rows.filter(status="error").count() if total else 0
        result.append({
            "id": j.id,
            "object_type": j.object_type,
            "status": j.status,
            "created_at": j.created_at.isoformat(),
            "filename": j.meta.get("original_filename", "unknown.csv"),
            "total_rows": total,
            "failed_rows": failed,
        })
    return result


@router.get("/{job_id}")
async def get_job(job_id: int, current_user: User = Depends(get_current_user)):
    job = await Job.get_or_none(id=job_id, user=current_user).prefetch_related("rows")
    if not job:
        raise HTTPException(404, "Job not found")

    total = len(job.rows)
    return {
        "id": job.id,
        "status": job.status,
        "object_type": job.object_type,
        "created_at": job.created_at.isoformat(),
        "meta": job.meta,
        "progress": {
            "total": total,
            "valid": await job.rows.filter(status="valid").count(),
            "error": await job.rows.filter(status="error").count(),
            "success": await job.rows.filter(status="success").count(),  # optional
        }
    }


@router.get("/{job_id}/errors")
async def download_errors(job_id: int, current_user: User = Depends(get_current_user)):
    job = await Job.get_or_none(id=job_id, user=current_user).prefetch_related("rows")
    if not job:
        raise HTTPException(404, "Job not found")

    errors = await job.rows.filter(status="error").values("row_number", "raw_data", "error")
    if not errors:
        return {"message": "No errors – all rows valid!"}

    output = io.StringIO()
    writer = csv.writer(output)
    # Use the keys from the first row's raw_data
    headers = ["Row #", "Error"] + list(errors[0]["raw_data"].keys())
    writer.writerow(headers)
    for e in errors:
        row = [e["row_number"], e["error"]] + [e["raw_data"].get(k, "") for k in errors[0]["raw_data"].keys()]
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=job_{job_id}_errors.csv"}
    )