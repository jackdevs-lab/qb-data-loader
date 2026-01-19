# New File: app/api/ai.py (For AI-Driven Features)
from fastapi import APIRouter, Depends, Body, HTTPException
from app.core.auth import get_current_user
from app.models.db import User
import openai  # Assume configured with env vars
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ai", tags=["ai"])

openai.api_key = "your-openai-key"  # Use env var in prod

@router.post("/suggest-mapping")
async def suggest_mapping(
    headers: list = Body(...),
    sample_rows: list = Body(...),
    current_user: User = Depends(get_current_user)
):
    try:
        prompt = f"Suggest QBO field mappings for CSV headers: {headers}. Sample data: {sample_rows[:2]}. Required: DisplayName. Output as JSON dict."
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        suggestions = response.choices[0].message.content
        # Parse JSON safely
        import json
        return json.loads(suggestions)
    except Exception as e:
        logger.error(f"AI mapping error: {str(e)}")
        raise HTTPException(500, "AI suggestion failed")

@router.post("/categorize")
async def categorize_expense(
    description: str = Body(...),
    amount: float = Body(...),
    current_user: User = Depends(get_current_user)
):
    try:
        prompt = f"Categorize expense: '{description}' for ${amount}. Suggest QBO account from standard chart (e.g., Office Supplies, Travel)."
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        category = response.choices[0].message.content.strip()
        return {"suggested_category": category}
    except Exception as e:
        logger.error(f"AI categorization error: {str(e)}")
        raise HTTPException(500, "AI categorization failed")

# Extend for more AI features like insights
