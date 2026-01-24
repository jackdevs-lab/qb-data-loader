from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

router = APIRouter(prefix="/api/expenses", tags=["expenses"])

@router.get("")
async def list_expenses(limit: int = Query(10), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        # Combined list: Expenses and Bills
        expense_query = f"SELECT * FROM Purchase WHERE TxnType = 'Expense' ORDERBY MetaData.CreateTime DESC MAXRESULTS {limit}"
        bill_query = f"SELECT * FROM Bill ORDERBY MetaData.CreateTime DESC MAXRESULTS {limit}"
        expense_resp = await client.get("/query", params={"query": expense_query, "minorversion": "75"})
        bill_resp = await client.get("/query", params={"query": bill_query, "minorversion": "75"})
        expense_resp.raise_for_status()
        bill_resp.raise_for_status()
        return {
            "expenses": expense_resp.json().get("QueryResponse", {}).get("Purchase", []),
            "bills": bill_resp.json().get("QueryResponse", {}).get("Bill", [])
        }
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.post("")
async def create_expense(payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post("/purchase", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.post("/bill")
async def create_bill(payload: dict, current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        resp = await client.post("/bill", json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()