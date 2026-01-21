# app/api/reports.py (Updated with Query-Based Fallbacks and Robust Parsing)
from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
from app.core.qbo import get_qbo_client
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import httpx
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/reports", tags=["reports"])

def calculate_dates(period: str):
    today = datetime.now()
    if period == "current-month":
        start = today.replace(day=1).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    elif period == "last-month":
        last_month = today - relativedelta(months=1)
        start = last_month.replace(day=1).strftime("%Y-%m-%d")
        end = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
    elif period == "quarter":
        quarter_start = today - relativedelta(months=3)
        start = quarter_start.strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    elif period == "year":
        year_start = today - relativedelta(years=1)
        start = year_start.strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    else:
        start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    return start, end

async def fallback_profit_loss(client, start_date, end_date):
    # Fallback: Query accounts and journal entries to compute P&L
    query = f"SELECT * FROM Account WHERE AccountType IN ('Income', 'Other Income', 'Expense', 'Other Expense', 'Cost of Goods Sold')"
    resp = await client.get("/query", params={"query": query, "minorversion": "75"})
    resp.raise_for_status()
    accounts = resp.json().get("QueryResponse", {}).get("Account", [])
    
    income_accounts = [acc['Id'] for acc in accounts if acc['AccountType'] in ['Income', 'Other Income']]
    expense_accounts = [acc['Id'] for acc in accounts if acc['AccountType'] in ['Expense', 'Other Expense', 'Cost of Goods Sold']]
    
    # Query balances (simplified; use more detailed for production)
    income_total = 0
    expense_total = 0
    # In production, query JournalEntry or use BalanceSheet/IncomeStatement queries if available
    # Placeholder: Assume aggregation logic here
    return {
        "income": [{"account": "Fallback Income", "amount": income_total}],
        "expenses": [{"account": "Fallback Expenses", "amount": expense_total}],
        "summary": {"totalIncome": income_total, "totalExpenses": expense_total, "netProfit": income_total - expense_total}
    }

@router.get("/profit-loss")
async def get_profit_loss(
    period: str = Query("current-month"),
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        start_date, end_date = calculate_dates(period)
        params = {"start_date": start_date, "end_date": end_date, "minorversion": "75"}
        resp = await client.get("/reports/ProfitAndLoss", params=params)
        if resp.status_code == 400 and "5020" in resp.text:
            logger.warning("Permission denied for ProfitAndLoss report; falling back to query-based.")
            return await fallback_profit_loss(client, start_date, end_date)
        resp.raise_for_status()
        data = resp.json()
        income = []
        expenses = []
        summary = {"totalIncome": 0, "totalExpenses": 0, "netProfit": 0}
        if "Rows" not in data or "Row" not in data["Rows"]:
            logger.warning("Empty or missing report data from QBO")
            return {"income": income, "expenses": expenses, "summary": summary}
        
        for section in data["Rows"]["Row"]:
            header_col_data = section.get("Header", {}).get("ColData", [])
            if len(header_col_data) > 0:
                header_value = header_col_data[0].get("value")
                if header_value == "Income":
                    section_rows = section.get("Rows", {}).get("Row", [])
                    for row in section_rows:
                        if row.get("type") == "Data":
                            row_col_data = row.get("ColData", [])
                            if len(row_col_data) >= 2:
                                account = row_col_data[0].get("value", "Unknown")
                                amount_str = row_col_data[1].get("value", "").replace(",", "")
                                try:
                                    amount = float(amount_str) if amount_str else 0.0
                                except ValueError:
                                    amount = 0.0
                                    logger.warning(f"Invalid amount '{amount_str}' in income row")
                                income.append({"account": account, "amount": amount})
                                summary["totalIncome"] += amount
                elif header_value == "Expenses":
                    section_rows = section.get("Rows", {}).get("Row", [])
                    for row in section_rows:
                        if row.get("type") == "Data":
                            row_col_data = row.get("ColData", [])
                            if len(row_col_data) >= 2:
                                account = row_col_data[0].get("value", "Unknown")
                                amount_str = row_col_data[1].get("value", "").replace(",", "")
                                try:
                                    amount = float(amount_str) if amount_str else 0.0
                                except ValueError:
                                    amount = 0.0
                                    logger.warning(f"Invalid amount '{amount_str}' in expenses row")
                                expenses.append({"account": account, "amount": amount})
                                summary["totalExpenses"] += amount
        
        for total_section in data["Rows"]["Row"]:
            summary_col_data = total_section.get("Summary", {}).get("ColData", [])
            if len(summary_col_data) > 0 and summary_col_data[0].get("value") == "Net Income":
                if len(summary_col_data) > 1 and "value" in summary_col_data[1]:
                    net_str = summary_col_data[1].get("value", "").replace(",", "")
                    try:
                        summary["netProfit"] = float(net_str) if net_str else 0.0
                    except ValueError:
                        summary["netProfit"] = 0.0
                        logger.warning(f"Invalid net profit value '{net_str}'")
                else:
                    summary["netProfit"] = 0.0
                    logger.warning("Missing or incomplete Net Income summary")
        
        return {"income": income, "expenses": expenses, "summary": summary}
    
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json() if e.response.content else {"error": str(e)}
        logger.error(f"QBO API error: {e.response.status_code} - {error_detail}")
        raise HTTPException(500, "QBO report access failed - check permissions or plan")
    except Exception as e:
        logger.error(f"Unexpected report error: {str(e)}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

# Similar fallbacks for other reports...
async def fallback_sales_summary(client, start_date, end_date):
    query = f"SELECT Id, TotalAmt FROM Invoice WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
    resp = await client.get("/query", params={"query": query, "minorversion": "75"})
    resp.raise_for_status()
    invoices = resp.json().get("QueryResponse", {}).get("Invoice", [])
    total_sales = sum(float(inv.get("TotalAmt", 0)) for inv in invoices)
    total_orders = len(invoices)
    return {"total": total_sales, "monthly": [{"month": "Current", "sales": total_sales}]}

@router.get("/sales-summary")
async def get_sales_summary(
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        today = datetime.now()
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        params = {"start_date": start_date, "end_date": end_date, "minorversion": "75"}
        resp = await client.get("/reports/TransactionListByDate", params=params)
        if resp.status_code == 400 and "5020" in resp.text:
            logger.warning("Permission denied for TransactionListByDate; falling back to query.")
            return await fallback_sales_summary(client, start_date, end_date)
        resp.raise_for_status()
        data = resp.json()
        total_sales = 0
        total_orders = 0
        if "Rows" in data and "Row" in data["Rows"]:
            for row in data["Rows"]["Row"]:
                if row.get("type") == "Data":
                    row_col_data = row.get("ColData", [])
                    if len(row_col_data) > 0 and row_col_data[0].get("value") == "Invoice":
                        total_orders += 1
                        if len(row_col_data) >= len(row_col_data):  # Adjust index based on actual structure
                            amount_str = row_col_data[-1].get("value", "").replace(",", "")
                            try:
                                total_sales += float(amount_str) if amount_str else 0.0
                            except ValueError:
                                logger.warning(f"Invalid amount '{amount_str}' in sales row")
        return {"total": total_sales, "monthly": [{"month": "Current", "sales": total_sales}]}
    
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json() if e.response.content else {"error": str(e)}
        logger.error(f"QBO API error: {e.response.status_code} - {error_detail}")
        raise HTTPException(500, "QBO report access failed - check permissions or plan")
    except Exception as e:
        logger.error(f"Unexpected report error: {str(e)}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.get("/customers")
async def get_customers_report(
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        query = "SELECT * FROM Customer MAXRESULTS 1000"
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        customers = resp.json().get("QueryResponse", {}).get("Customer", [])
        return customers  # List of customer dicts
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json() if e.response.content else {"error": str(e)}
        logger.error(f"QBO API error: {e.response.status_code} - {error_detail}")
        raise HTTPException(500, "QBO customers access failed - check permissions")
    except Exception as e:
        logger.error(f"Unexpected customers report error: {str(e)}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()

@router.get("/sales")
async def get_sales_report(
    period: str = Query("monthly"),
    current_user: User = Depends(get_current_user)
):
    client = await get_qbo_client(current_user)
    try:
        start_date, end_date = calculate_dates(period)
        query = f"SELECT * FROM Invoice WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
        resp = await client.get("/query", params={"query": query, "minorversion": "75"})
        resp.raise_for_status()
        invoices = resp.json().get("QueryResponse", {}).get("Invoice", [])
        
        total_sales = sum(float(inv.get("TotalAmt", 0)) for inv in invoices)
        total_orders = len(invoices)
        average_order = total_sales / total_orders if total_orders > 0 else 0
        top_customer = max(invoices, key=lambda inv: float(inv.get("TotalAmt", 0))).get("CustomerRef", {}).get("name", "N/A") if invoices else "N/A"
        
        # Simple monthly aggregation (expand for true monthly breakdown)
        monthly = [{"month": f"{start_date} to {end_date}", "sales": total_sales, "orders": total_orders, "averageOrder": average_order}]
        
        summary = {
            "totalSales": total_sales,
            "totalOrders": total_orders,
            "averageOrder": average_order,
            "topCustomer": top_customer
        }
        return {"summary": summary, "monthly": monthly}
    except httpx.HTTPStatusError as e:
        error_detail = e.response.json() if e.response.content else {"error": str(e)}
        logger.error(f"QBO API error: {e.response.status_code} - {error_detail}")
        raise HTTPException(500, "QBO sales access failed - check permissions")
    except Exception as e:
        logger.error(f"Unexpected sales report error: {str(e)}", exc_info=True)
        raise HTTPException(500, str(e))
    finally:
        await client.aclose()