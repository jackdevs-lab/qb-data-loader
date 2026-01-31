# app/api/customers.py
from fastapi import APIRouter, Depends, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client
import httpx
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/customers", tags=["customers"])

@router.get("")
async def list_customers(
    limit: int | None = None,
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        max_results = limit or 1000
        query = f"SELECT * FROM Customer WHERE Active IN (true, false) MAXRESULTS {max_results}"
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        data = resp.json()
        return data.get("QueryResponse", {}).get("Customer", [])
    finally:
        await client.aclose()


@router.get("/{customer_id}")
async def get_customer(
    customer_id: str,
    current_user: User = Depends(get_current_user)
):
    """Fetch single customer – required for edit form (includes latest SyncToken)"""
    client = await get_qbo_client(current_user)
    try:
        resp = await client.get(f"/customer/{customer_id}", params={"minorversion": "75"})
        resp.raise_for_status()
        customer = resp.json().get("Customer")
        if not customer:
            raise HTTPException(404, "Customer not found")
        return customer
    finally:
        await client.aclose()


@router.post("")
async def create_customer(
    payload: dict,
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post("/customer", json=payload)
        resp.raise_for_status()
        return resp.json()
    finally:
        await client.aclose()


def unflatten_dict(d: dict) -> dict:
    """
    Converts a flat dictionary with dot-notated keys into a nested dictionary.
    Example: {"BillAddr.Line1": "123 Main"} -> {"BillAddr": {"Line1": "123 Main"}}
    """
    result = {}
    for key, value in d.items():
        parts = key.split('.')
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result

@router.put("/{customer_id}")
async def update_customer(
    customer_id: str,
    payload: dict,                     # Sparse update payload from frontend
    current_user: User = Depends(get_current_user)
):
    """Sparse update – QuickBooks requires Id and SyncToken"""
    client = await get_qbo_client(current_user)
    try:
        # 1. Unflatten dot-notation keys from frontend (e.g. "BillAddr.Line1")
        nested_payload = unflatten_dict(payload)

        # 2. QuickBooks expects the Customer object directly, NOT wrapped in {"Customer": ...}
        # for sparse updates via POST.
        full_payload = {
            **nested_payload,
            "Id": customer_id,
            "sparse": True
        }
        
        if "SyncToken" not in full_payload:
            current = await get_customer(customer_id, current_user)
            full_payload["SyncToken"] = current["SyncToken"]

        logger.info(f"Sending QuickBooks update for customer {customer_id}: {full_payload}")
        
        resp = await client.post(
            "/customer?operation=update&minorversion=75",
            json=full_payload
        )
        
        if resp.is_error:
            error_text = resp.text
            logger.error(f"QuickBooks API Error (Status {resp.status_code}): {error_text}")
            raise HTTPException(status_code=resp.status_code, detail=f"QuickBooks Error: {error_text}")
            
        return resp.json()
    except httpx.HTTPStatusError as e:
        error_detail = e.response.text
        logger.error(f"QuickBooks HTTPStatusError: {error_detail}")
        raise HTTPException(status_code=e.response.status_code, detail=error_detail)
    finally:
        await client.aclose()


@router.delete("/{customer_id}")
async def delete_customer(
    customer_id: str,
    current_user: User = Depends(get_current_user)
):
    """Soft delete → sets Active = false in QuickBooks"""
    try:
        # Delegate to update_customer to ensure consistent sparse update logic
        await update_customer(customer_id, {"Active": False}, current_user)
        return {"success": True, "message": "Customer deactivated successfully"}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in delete_customer: {str(e)}")
        raise HTTPException(500, detail=str(e))