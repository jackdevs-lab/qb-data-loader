# app/schemas/customer.py

from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import re


class Phone(BaseModel):
    FreeFormNumber: Optional[str] = Field(None, max_length=30)

    @validator("FreeFormNumber", pre=True, always=True)
    def clean_phone(cls, v):
        if isinstance(v, str):
            cleaned = v.strip()
            return cleaned if cleaned else None
        return None


class Email(BaseModel):
    Address: Optional[EmailStr] = None

    @validator("Address")
    def strict_email_validation(cls, v):
        if not v:
            return None
        v = v.strip()
        if v.endswith('.'):
            raise ValueError("Email domain cannot end with a dot")
        if re.search(r'\.\.', v):
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
        if not v:
            return None

        parsed = urlparse(v)
        if not parsed.scheme:
            v = "https://" + v
            parsed = urlparse(v)

        if parsed.scheme in ("http", "https") and parsed.netloc:
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


US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
}


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
    PrimaryEmailAddr: Optional["Email"] = None
    PrimaryPhone: Optional["Phone"] = None
    Mobile: Optional["Phone"] = None
    Fax: Optional["Phone"] = None
    AlternatePhone: Optional["Phone"] = None
    WebAddr: Optional["WebAddr"] = None
    BillAddr: Optional["Address"] = None
    ShipAddr: Optional["Address"] = None
    # === Addresses ===
    BillAddr: Optional[Address] = None
    ShipAddr: Optional[Address] = None

    # === Common fields ===
    Notes: Optional[str] = Field(None, max_length=2000)
    Taxable: Optional[bool] = None
    Active: Optional[bool] = True
    Job: Optional[bool] = False
    BillWithParent: Optional[bool] = None
    ParentRef: Optional[Dict[str, str]] = None
    CurrencyRef: Optional[Dict[str, str]] = None
    PrintOnCheckName: Optional[str] = Field(None, max_length=100)

    # Global cleaner: empty strings -> None
    @validator("*", pre=True, always=True)
    def global_empty_to_none(cls, v):
        return None if isinstance(v, str) and v.strip() == "" else v

    def to_qbo_payload(self) -> Dict[str, Any]:
        """
        Convert to QuickBooks Online compatible payload for CREATE operation.
        Returns: {"Customer": { ... valid fields ... }}
        Safe for nested fields being either Pydantic models or plain dicts.
        """
        # Exclude unset and None values
        data = self.model_dump(exclude_unset=True, exclude_none=True)

        inner: Dict[str, Any] = {"DisplayName": data.pop("DisplayName")}

        # Direct scalar fields
        scalar_fields = [
            "CompanyName", "Title", "GivenName", "MiddleName", "FamilyName",
            "Suffix", "Notes", "Taxable", "Active", "Job", "BillWithParent",
            "ParentRef", "CurrencyRef", "PrintOnCheckName"
        ]
        for field in scalar_fields:
            if field in data:
                inner[field] = data.pop(field)

        # Nested objects
        nested_mapping = {
            "PrimaryEmailAddr": "PrimaryEmailAddr",
            "PrimaryPhone": "PrimaryPhone",
            "Mobile": "Mobile",
            "Fax": "Fax",
            "AlternatePhone": "AlternatePhone",
            "WebAddr": "WebAddr",
            "BillAddr": "BillAddr",
            "ShipAddr": "ShipAddr",
        }

        for model_field, qbo_field in nested_mapping.items():
            if model_field not in data:
                continue
            obj = data.pop(model_field)

            # === THE FIX: Handle both Pydantic model and plain dict ===
            if hasattr(obj, "model_dump"):
                nested_dict = obj.model_dump(exclude_none=True)
            else:
                # It's already a dict — just filter out None values
                nested_dict = {k: v for k, v in obj.items() if v is not None}

            # Special handling for addresses
            if model_field in ("BillAddr", "ShipAddr"):
                if not nested_dict.get("Line1"):
                    continue  # QBO ignores addresses without Line1
                # Auto-add Country if missing and we can infer USA
                if "Country" not in nested_dict:
                    state = nested_dict.get("CountrySubDivisionCode", "").strip().upper()
                    if state in US_STATES:
                        nested_dict["Country"] = "USA"

            if nested_dict:  # Only include if not empty
                inner[qbo_field] = nested_dict

        # Add any remaining fields (safety net)
        inner.update(data)

        return {"Customer": inner}