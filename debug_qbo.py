
import asyncio
import httpx
import json
from app.models.db import User
from app.core.db import TORTOISE_ORM
from tortoise import Tortoise

async def test_qbo():
    await Tortoise.init(config=TORTOISE_ORM)
    results = {}
    try:
        user = await User.all().first()
        if not user:
            results["error"] = "No user found"
            return
        
        from app.core.qbo import get_qbo_client
        client = await get_qbo_client(user)
        try:
            # Test Purchase
            q = "SELECT * FROM Purchase ORDER BY MetaData.CreateTime DESC MAXRESULTS 1"
            resp = await client.get("/query", params={"query": q, "minorversion": "75"})
            results["Purchase"] = {"status": resp.status_code, "text": resp.text}

            # Test Bill
            q = "SELECT * FROM Bill ORDER BY MetaData.CreateTime DESC MAXRESULTS 1"
            resp = await client.get("/query", params={"query": q, "minorversion": "75"})
            results["Bill"] = {"status": resp.status_code, "text": resp.text}

            # Test SalesReceipt
            q = "SELECT * FROM SalesReceipt ORDER BY MetaData.CreateTime DESC MAXRESULTS 1"
            resp = await client.get("/query", params={"query": q, "minorversion": "75"})
            results["SalesReceipt"] = {"status": resp.status_code, "text": resp.text}

        finally:
            await client.aclose()
    except Exception as e:
        results["exception"] = str(e)
    finally:
        await Tortoise.close_connections()
        with open("debug_results.json", "w") as f:
            json.dump(results, f, indent=2)

if __name__ == "__main__":
    asyncio.run(test_qbo())
