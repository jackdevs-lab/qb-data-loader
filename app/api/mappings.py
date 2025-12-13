from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime
from app.models.db import MappingTemplate
from app.api.imports import get_dev_user  # Reuse your dev user helper

router = APIRouter(tags=["mappings"])  # No prefix here
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
async def create_mapping(data: MappingCreate, user=Depends(get_dev_user)):
    template = await MappingTemplate.create(
        user=user,
        name=data.name,
        object_type=data.object_type,
        mapping=data.mapping
    )
    return template

@router.get("/", response_model=List[MappingResponse])
async def list_mappings(user=Depends(get_dev_user)):
    return await MappingTemplate.filter(user=user).order_by("-created_at")