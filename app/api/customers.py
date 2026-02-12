from fastapi import APIRouter, Depends, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client
from app.schemas.customer import CustomerCanonical, CustomerUpdate
import httpx
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/customers", tags=["customers"])

@router.get("")
async def list_customers(
    limit: int = 100,
    offset: int = 1,
    include_count: bool = True,
    current_user: User = Depends(get_current_user)
):
    """List customers with pagination support"""
    client = await get_qbo_client(current_user)
    try:
        # 1. Fetch data
        query = f"SELECT * FROM Customer WHERE Active IN (true, false) STARTPOSITION {offset} MAXRESULTS {limit}"
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        data = resp.json()
        customers = data.get("QueryResponse", {}).get("Customer", [])
        
        # 2. Fetch total count if requested
        total_count = None
        if include_count:
            count_query = "SELECT COUNT(*) FROM Customer WHERE Active IN (true, false)"
            count_resp = await client.get("/query", params={"query": count_query, "minorversion": "75"})
            if count_resp.is_success:
                total_count = count_resp.json().get("QueryResponse", {}).get("totalCount")

        return {
            "customers": customers,
            "totalCount": total_count if total_count is not None else len(customers),
            "limit": limit,
            "offset": offset,
            "hasMore": len(customers) == limit
        }
    except Exception as e:
        logger.error(f"Error listing customers: {str(e)}")
        raise HTTPException(500, detail="Failed to fetch customers from QuickBooks")
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
    customer: CustomerCanonical,
    current_user: User = Depends(get_current_user)
):
    """Create a new customer with full validation"""
    client = await get_qbo_client(current_user)
    try:
        payload = {"Customer": customer.to_qbo_payload()}
        logger.info(f"Creating QuickBooks customer: {customer.DisplayName}")
        
        resp = await client.post("/customer?minorversion=75", json=payload)
        
        if resp.is_error:
            error_text = resp.text
            logger.error(f"QuickBooks API Error (Create Status {resp.status_code}): {error_text}")
            raise HTTPException(status_code=resp.status_code, detail=f"QuickBooks Error: {error_text}")
            
        return resp.json()
    except httpx.HTTPStatusError as e:
        error_detail = e.response.text
        logger.error(f"QuickBooks HTTPStatusError during create: {error_detail}")
        raise HTTPException(status_code=e.response.status_code, detail=error_detail)
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
    payload: dict,                     # We still take dict to handle dot-notation unflattening
    current_user: User = Depends(get_current_user)
):
    """Sparse update with validation and SyncToken auto-fetch if missing"""
    client = await get_qbo_client(current_user)
    try:
        # 1. Unflatten dot-notation keys from frontend
        nested_payload = unflatten_dict(payload)
        
        # 2. Add ID to payload
        nested_payload["Id"] = customer_id
        
        # 3. Handle SyncToken
        if "SyncToken" not in nested_payload:
            current = await get_customer(customer_id, current_user)
            nested_payload["SyncToken"] = current["SyncToken"]

        # 4. Validate via Schema
        update_model = CustomerUpdate(**nested_payload)
        full_payload = update_model.to_qbo_payload()

        logger.info(f"Sending QuickBooks update for customer {customer_id}: {full_payload}")
        
        resp = await client.post(
            "/customer?operation=update&minorversion=75",
            json=full_payload
        )
        
        if resp.is_error:
            error_text = resp.text
            logger.error(f"QuickBooks API Error (Update Status {resp.status_code}): {error_text}")
            raise HTTPException(status_code=resp.status_code, detail=f"QuickBooks Error: {error_text}")
            
        return resp.json()
    except httpx.HTTPStatusError as e:
        error_detail = e.response.text
        logger.error(f"QuickBooks HTTPStatusError during update: {error_detail}")
        raise HTTPException(status_code=e.response.status_code, detail=error_detail)
    except ValueError as e:
        logger.error(f"Validation error during customer update: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))
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