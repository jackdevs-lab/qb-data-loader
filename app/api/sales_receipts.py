# sales_receipts.py (full updated version)
from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

router = APIRouter(prefix="/api/sales_receipts", tags=["sales_receipts"])

@router.get("")
async def list_sales_receipts(limit: int = Query(10), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        query = f"SELECT * FROM SalesReceipt ORDER BY MetaData.CreateTime DESC MAXRESULTS {limit}"
        print(f"DEBUG: Running sales receipt query: {query}")
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        data = resp.json()
        print(f"DEBUG: Sales receipt response: {data}")
        return data.get("QueryResponse", {}).get("SalesReceipt", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.get("/{receipt_id}")
async def get_sales_receipt(receipt_id: str, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.get(f"/salesreceipt/{receipt_id}", params={"minorversion": "75"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.post("")
async def create_sales_receipt(payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post("/salesreceipt", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.put("/{receipt_id}")
async def update_sales_receipt(receipt_id: str, payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        payload["Id"] = receipt_id
        if "sparse" not in payload:
            payload["sparse"] = True
        resp = await client.post("/salesreceipt", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()

@router.delete("/{receipt_id}")
async def void_sales_receipt(receipt_id: str, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        # Fetch current object to get SyncToken
        get_resp = await client.get(f"/salesreceipt/{receipt_id}", params={"minorversion": "75"})
        get_resp.raise_for_status()
        current = get_resp.json().get("SalesReceipt")

        payload = {
            "Id": receipt_id,
            "SyncToken": current["SyncToken"],
            "sparse": True
        }
        resp = await client.post("/salesreceipt", params={"operation": "update", "include": "void"}, json=payload)
        resp.raise_for_status()
        return {"message": "Sales Receipt voided successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await client.aclose()