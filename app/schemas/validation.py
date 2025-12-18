# app/schemas/validation.py

from pydantic import BaseModel
from typing import List, Optional, Literal, Dict, Any

class ValidationIssue(BaseModel):
    level: Literal["error", "warning"]
    code: str
    message: str
    field: Optional[str] = None
    row: Optional[int] = None

class RowValidationResult(BaseModel):
    row_number: int
    status: Literal["valid", "warning", "error"]
    issues: List[ValidationIssue] = []
    normalized_data: Optional[Dict[str, Any]] = None

class DryRunSummary(BaseModel):
    total_rows: int
    will_succeed: int
    will_fail: int
    warnings: int
    issues: List[ValidationIssue] = []