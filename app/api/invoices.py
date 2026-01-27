# invoices.py (full updated version)
from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

router = APIRouter(prefix="/api/invoices", tags=["invoices"])

@router.get("")
async def list_invoices(limit: int = Query(10), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
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

@router.get("/{invoice_id}")
async def get_invoice(invoice_id: str, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.get(f"/invoice/{invoice_id}", params={"minorversion": "75"})
        resp.raise_for_status()
        return resp.json().get("Invoice")
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

@router.put("/{invoice_id}")
async def update_invoice(invoice_id: str, payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        payload["Id"] = invoice_id
        if "sparse" not in payload:
            payload["sparse"] = True
        resp = await client.post("/invoice", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.delete("/{invoice_id}")
async def void_invoice(invoice_id: str, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        # Fetch current object to get SyncToken
        get_resp = await client.get(f"/invoice/{invoice_id}", params={"minorversion": "75"})
        get_resp.raise_for_status()
        current = get_resp.json().get("Invoice")

        payload = {
            "Id": invoice_id,
            "SyncToken": current["SyncToken"],
            "sparse": True
        }
        resp = await client.post("/invoice", params={"operation": "update", "include": "void"}, json=payload)
        resp.raise_for_status()
        return {"message": "Invoice voided successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()