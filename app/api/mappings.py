# app/api/mappings.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime

from app.models.db import MappingTemplate, User
from app.core.auth import get_current_user  # ← Proper authenticated user

router = APIRouter(tags=["mappings"])  # No prefix, as in your original


class MappingCreate(BaseModel):
    name: str
    object_type: str
    mapping: Dict[str, str]  # {"CSV_Header": "QBO.Field.Name"}


class MappingResponse(BaseModel):
    id: int
    name: str
    object_type: str
    mapping: Dict[str, str]
    created_at: datetime


@router.post("/", response_model=MappingResponse)
async def create_mapping(
    data: MappingCreate,
    user: User = Depends(get_current_user)  # ← Real logged-in user
):
    template = await MappingTemplate.create(
        user=user,
        name=data.name,
        object_type=data.object_type,
        mapping=data.mapping
    )
    return template


@router.get("/", response_model=List[MappingResponse])
async def list_mappings(
    user: User = Depends(get_current_user)  # ← Real logged-in user
):
    return await MappingTemplate.filter(user=user).order_by("-created_at")