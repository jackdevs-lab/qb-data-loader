from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

router = APIRouter(prefix="/api/invoices", tags=["invoices"])

@router.get("")
async def list_invoices(limit: int = Query(10), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        # Fixed: space in "ORDER BY"
        query = f"SELECT * FROM Invoice ORDER BY MetaData.CreateTime DESC MAXRESULTS {limit}"
        print(f"DEBUG: Running invoice query: {query}")
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        data = resp.json()
        print(f"DEBUG: Invoice response: {data}")
        return data.get("QueryResponse", {}).get("Invoice", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.post("")
async def create_invoice(payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post("/invoice", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()