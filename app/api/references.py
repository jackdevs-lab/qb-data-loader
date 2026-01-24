from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client

router = APIRouter(prefix="/api/references", tags=["references"])

async def query_qbo(client, entity: str, where: str = "", limit: int = 1000):
    query = f"SELECT * FROM {entity} {where} MAXRESULTS {limit}"
    resp = await client.get("/query", params={"query": query, "minorversion": "75"})
    resp.raise_for_status()
    return resp.json().get("QueryResponse", {}).get(entity, [])

@router.get("/accounts")
async def list_accounts(limit: int = Query(1000), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        return await query_qbo(client, "Account")
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.get("/vendors")
async def list_vendors(limit: int = Query(1000), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        return await query_qbo(client, "Vendor")
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.get("/paymentmethods")
async def list_paymentmethods(limit: int = Query(1000), current_user: User = Depends(get_current_user)):
    client = await get_qbo_client(current_user)
    try:
        return await query_qbo(client, "PaymentMethod")
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()