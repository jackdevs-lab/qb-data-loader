# app/api/customers.py
from fastapi import APIRouter, Depends, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

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


@router.put("/{customer_id}")
async def update_customer(
    customer_id: str,
    payload: dict,                     # Sparse update payload from frontend
    current_user: User = Depends(get_current_user)
):
    """Sparse update – QuickBooks requires Id and SyncToken"""
    client = await get_qbo_client(current_user)
    try:
        # QuickBooks expects the full Customer object with Id and SyncToken for updates
        update_wrapper = {
            "Customer": {
                **payload,
                "Id": customer_id,
                "sparse": True                  # This enables sparse updates
            }
        }
        if "SyncToken" not in update_wrapper["Customer"]:
            # Safety: fetch latest SyncToken if frontend forgot (not ideal, but prevents 400 errors)
            current = await get_customer(customer_id, current_user)
            update_wrapper["Customer"]["SyncToken"] = current["SyncToken"]

        resp = await client.post(f"/customer?operation=update", json=update_wrapper)
        resp.raise_for_status()
        return resp.json()
    finally:
        await client.aclose()


@router.delete("/{customer_id}")
async def delete_customer(
    customer_id: str,
    current_user: User = Depends(get_current_user)
):
    """Soft delete → sets Active = false in QuickBooks"""
    client = await get_qbo_client(current_user)
    try:
        # First get latest SyncToken (required for update/delete)
        current = await get_customer(customer_id, current_user)
        payload = {
            "Id": customer_id,
            "SyncToken": current["SyncToken"],
            "Active": False,
            "sparse": True
        }
        resp = await client.post(f"/customer?operation=update", json={"Customer": payload})
        resp.raise_for_status()
        return {"success": true, "message": "Customer deactivated successfully"}
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(404, "Customer not found")
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()