# app/api/customers.py
from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client
import httpx

router = APIRouter(prefix="/api/customers", tags=["customers"])

@router.get("")
async def list_customers(
    limit: int = Query(None),
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        max_results = limit if limit else 1000
        query = f"SELECT * FROM Customer MAXRESULTS {max_results}"
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        data = resp.json()
        return data.get("QueryResponse", {}).get("Customer", [])
    except Exception as e:
        raise HTTPException(500, str(e))
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
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.delete("/{customer_id}")
async def delete_customer(
    customer_id: str,
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post(f"/customer?operation=delete", json={"Id": customer_id})
        resp.raise_for_status()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()