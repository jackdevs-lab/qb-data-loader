# app/schemas/validators.py   ← create this new file
from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional

class CustomerRow(BaseModel):
    DisplayName: str = Field(..., min_length=1, max_length=100)
    PrimaryEmailAddr: Optional[EmailStr] = None
    BillAddr_Line1: Optional[str] = None
    BillAddr_City: Optional[str] = None
    BillAddr_PostalCode: Optional[str] = None

    @validator("DisplayName")
    def no_double_spaces(cls, v):
        if "  " in v:
            raise ValueError("double spaces not allowed")
        return v.strip()

# Add more later (InvoiceRow, ItemRow, etc.)
VALIDATORS = {
    "customer": CustomerRow,
    "customers": CustomerRow,   # alias
}