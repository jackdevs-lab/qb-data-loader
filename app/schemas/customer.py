# app/schemas/customer.py

from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Dict, Any
from urllib.parse import urlparse
from pydantic.networks import EmailStr
import re


class Phone(BaseModel):
    FreeFormNumber: Optional[str] = Field(None, max_length=30)

    @validator("FreeFormNumber", pre=True, always=True)
    def clean_phone(cls, v):
        if isinstance(v, str):
            cleaned = v.strip()
            if cleaned == "":
                return None
            return cleaned
        return None if v is None else str(v).strip() or None


class Email(BaseModel):
    Address: Optional[EmailStr] = None

    @validator("Address")
    def strict_email_validation(cls, v):
        if not v:
            return None
        # Extra strict check: no trailing dot in domain
        if v.strip().endswith('.'):
            raise ValueError("Email domain cannot end with a dot")
        # Also reject common invalid patterns QBO hates
        if re.search(r'\.\.', v):  # double dot
            raise ValueError("Email contains invalid double dot")
        if v.count('@') != 1:
            raise ValueError("Email must contain exactly one @")
        return v


class WebAddr(BaseModel):
    URI: Optional[str] = Field(None, max_length=2000)

    @validator("URI", pre=True, always=True)
    def validate_and_clean_url(cls, v):
        if not v:
            return None
        if not isinstance(v, str):
            v = str(v)
        v = v.strip()
        if v == "":
            return None

        # Add https:// if no scheme
        parsed = urlparse(v)
        if not parsed.scheme:
            v = "https://" + v
            parsed = urlparse(v)

        if parsed.netloc and parsed.scheme in ("http", "https"):
            return v

        raise ValueError("Invalid website URL format")


class Address(BaseModel):
    Line1: Optional[str] = Field(None, max_length=500)
    Line2: Optional[str] = Field(None, max_length=500)
    Line3: Optional[str] = Field(None, max_length=500)
    City: Optional[str] = Field(None, max_length=100)
    CountrySubDivisionCode: Optional[str] = Field(None, max_length=100)
    PostalCode: Optional[str] = Field(None, max_length=30)
    Country: Optional[str] = Field(None, max_length=100)

    @validator("*", pre=True, always=True)
    def empty_to_none(cls, v):
        return None if isinstance(v, str) and v.strip() == "" else v


class CustomerCanonical(BaseModel):
    # === Required ===
    DisplayName: str = Field(..., min_length=1, max_length=500)

    # === Name components ===
    CompanyName: Optional[str] = Field(None, max_length=500)
    Title: Optional[str] = Field(None, max_length=16)
    GivenName: Optional[str] = Field(None, max_length=100)
    MiddleName: Optional[str] = Field(None, max_length=100)
    FamilyName: Optional[str] = Field(None, max_length=100)
    Suffix: Optional[str] = Field(None, max_length=16)

    # === Contact ===
    # FIX: Use string annotations to avoid forward reference issues
    PrimaryEmailAddr: Optional["Email"] = None
    PrimaryPhone: Optional["Phone"] = None
    Mobile: Optional["Phone"] = None
    Fax: Optional["Phone"] = None
    AlternatePhone: Optional["Phone"] = None
    WebAddr: Optional["WebAddr"] = None

    # === Addresses ===
    BillAddr: Optional["Address"] = None
    ShipAddr: Optional["Address"] = None

    # === Common fields ===
    Notes: Optional[str] = Field(None, max_length=2000)
    Taxable: Optional[bool] = None
    Active: Optional[bool] = True
    Job: Optional[bool] = False
    BillWithParent: Optional[bool] = None
    ParentRef: Optional[Dict[str, str]] = None
    CurrencyRef: Optional[Dict[str, str]] = None

    # Global empty string cleaner (fallback)
    @validator("*", pre=True, always=True)
    def global_empty_to_none(cls, v):
        return None if isinstance(v, str) and v.strip() == "" else v

    def to_qbo_payload(self) -> Dict[str, Any]:
        data = self.dict(exclude_unset=True, exclude_none=True)

        payload = {"DisplayName": data.pop("DisplayName")}

        # Scalars
        scalar_fields = [
            "CompanyName", "Title", "GivenName", "MiddleName", "FamilyName",
            "Suffix", "Notes", "Taxable", "Active", "Job", "BillWithParent",
            "ParentRef", "CurrencyRef"
        ]
        for f in scalar_fields:
            if f in data:
                payload[f] = data.pop(f)

        # Nested objects
        for nested in ["PrimaryEmailAddr", "PrimaryPhone", "Mobile", "Fax", "AlternatePhone", "WebAddr", "BillAddr", "ShipAddr"]:
            if nested in data:
                obj = data.pop(nested)
            payload[nested] = obj.dict(exclude_none=True) if hasattr(obj, "dict") else obj

        payload.update(data)
        return payload