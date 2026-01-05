import re
import datetime
from pydantic import BaseModel, EmailStr, field_validator, ValidationInfo
from typing import Optional, Any
from urllib.parse import urlparse
from pydantic.fields import Field
from typing import Dict, Any, Optional
from datetime import datetime

# US States (2-letter codes)
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
}

# Canadian Provinces
CANADA_PROVINCES = {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}

class EmailAddress(BaseModel):
    Address: Optional[EmailStr] = None

    @field_validator("Address", mode="before")
    @classmethod
    def strict_email_validation(cls, v: Any) -> Optional[str]:
        if not v:
            return None
        v = str(v).strip()
        if not v:
            return None
        junk = ["invalid email", "n/a", "none", "-", "no email"]
        if v.lower() in junk:
            raise ValueError("Invalid/junk email provided")
        if v.endswith('.'):
            raise ValueError("Domain cannot end with .")
        if re.search(r'\.\.', v):
            raise ValueError("No double dots in domain")
        if v.count('@') != 1:
            raise ValueError("Must have exactly one @")
        return v


class Website(BaseModel):
    URI: Optional[str] = None

    @field_validator("URI", mode="before")
    @classmethod
    def validate_and_clean_url(cls, v: Any) -> Optional[str]:
        if not v:
            return None
        v = str(v).strip()
        if not v:
            return None
        junk = ["invalid website", "n/a", "none", "-", "no website"]
        if v.lower() in junk:
            raise ValueError("Invalid/junk website")
        if not v.startswith(('http://', 'https://')):
            v = 'https://' + v
        parsed = urlparse(v)
        if not (parsed.scheme in ('http', 'https') and parsed.netloc and '.' in parsed.netloc):
            raise ValueError("Must be a valid URL")
        return parsed.geturl()


class PhoneNumber(BaseModel):
    FreeFormNumber: Optional[str] = Field(None, max_length=30)

    @field_validator("FreeFormNumber", mode="before")
    @classmethod
    def clean_phone(cls, v: Any) -> Optional[str]:
        if not v:
            return None
        v = str(v).strip()
        if not v:
            return None
        return v
class PhysicalAddress(BaseModel):
    Line1: Optional[str] = Field(None, max_length=500)
    Line2: Optional[str] = Field(None, max_length=500)
    Line3: Optional[str] = Field(None, max_length=500)
    Line4: Optional[str] = Field(None, max_length=500)
    Line5: Optional[str] = Field(None, max_length=500)
    City: Optional[str] = Field(None, max_length=100)
    CountrySubDivisionCode: Optional[str] = Field(None, max_length=100)  # State/Province
    PostalCode: Optional[str] = Field(None, max_length=30)
    Country: Optional[str] = Field("USA", max_length=100)  # Default USA

    @field_validator("*", mode="before")
    @classmethod
    def empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and v.strip() == "" else v

    @field_validator("PostalCode")
    @classmethod
    def validate_postal_code(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        if not v:
            return v
        v = v.strip().upper()
        country = (info.data.get("Country") or "USA").upper()

        if country in ("USA", "US"):
            if not re.match(r"^\d{5}(-\d{4})?$", v):
                raise ValueError("US ZIP must be 12345 or 12345-6789")
        elif country == "CANADA":
            clean = v.replace(" ", "")
            if not re.match(r"^[A-Z]\d[A-Z]\d[A-Z]\d$", clean):
                raise ValueError("Canadian postal code must be A1A 1A1 format")
        elif country == "MEXICO":
            if not re.match(r"^\d{5}$", v):
                raise ValueError("Mexican postal code must be 5 digits")
        # Add more countries if needed
        return v

    @field_validator("CountrySubDivisionCode")
    @classmethod
    def validate_state_province(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        if not v:
            return v
        v = v.strip().upper()
        country = (info.data.get("Country") or "USA").upper()

        if country in ("USA", "US"):
            if v not in US_STATES:
                raise ValueError(f"Invalid US state code: {v}")
        elif country == "CANADA":
            if v not in CANADA_PROVINCES:
                raise ValueError(f"Invalid Canadian province code: {v}")
        # Add more if needed
        return v

class CustomerCanonical(BaseModel):
    DisplayName: str = Field(..., min_length=1, max_length=500)
    CompanyName: Optional[str] = Field(None, max_length=500)
    Title: Optional[str] = Field(None, max_length=16)
    GivenName: Optional[str] = Field(None, max_length=100)
    MiddleName: Optional[str] = Field(None, max_length=100)
    FamilyName: Optional[str] = Field(None, max_length=100)
    Suffix: Optional[str] = Field(None, max_length=16)
    PrintOnCheckName: Optional[str] = Field(None, max_length=110)
    Notes: Optional[str] = Field(None, max_length=2000)

    PrimaryEmailAddr: Optional[EmailAddress] = None
    PrimaryPhone: Optional[PhoneNumber] = None
    Mobile: Optional[PhoneNumber] = None
    Fax: Optional[PhoneNumber] = None
    AlternatePhone: Optional[PhoneNumber] = None
    WebAddr: Optional[Website] = None
    BillAddr: Optional[PhysicalAddress] = None
    ShipAddr: Optional[PhysicalAddress] = None

    CurrencyRef: Optional[Dict[str, str]] = None
    Balance: Optional[float] = None
    OpenBalanceDate: Optional[str] = None
    Taxable: Optional[bool] = None
    Active: Optional[bool] = True

    @field_validator("*", mode="before")
    @classmethod
    def global_empty_to_none(cls, v: Any) -> Any:
        return None if isinstance(v, str) and v.strip() == "" else v

    @field_validator("DisplayName", "Title", "GivenName", "MiddleName", "FamilyName",
                     "Suffix", "PrintOnCheckName", "CompanyName")
    @classmethod
    def no_prohibited_chars(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        if v and any(c in v for c in ":\t\n"):
            raise ValueError(f"{info.field_name} cannot contain : , tab, or newline")
        return v

    @field_validator("OpenBalanceDate", mode="before")
    @classmethod
    def validate_open_balance_date(cls, v: Any) -> Optional[str]:
        if not v:
            return None
        v = str(v).strip()
        formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y"]
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(v, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        raise ValueError("Invalid date format. Supported: YYYY-MM-DD, MM/DD/YYYY, etc.")
    def to_qbo_payload(self) -> Dict[str, Any]:
        data = self.model_dump(exclude_unset=True, exclude_none=True, by_alias=True)
        inner: Dict[str, Any] = {"DisplayName": data.pop("DisplayName")}

        scalar_fields = [
            "CompanyName", "Title", "GivenName", "MiddleName", "FamilyName",
            "Suffix", "Notes", "Taxable", "Active", "Job", "BillWithParent",
            "ParentRef", "CurrencyRef", "PrintOnCheckName", "ResaleNum",
            "BusinessNumber", "GSTIN", "PrimaryTaxIdentifier",
            "PreferredDeliveryMethod", "Balance", "OpenBalanceDate"
        ]
        for field in scalar_fields:
            if field in data:
                inner[field] = data.pop(field)

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

            if hasattr(obj, "model_dump"):
                nested_dict = obj.model_dump(exclude_none=True, by_alias=True)
            else:
                nested_dict = {k: v for k, v in obj.items() if v is not None}

            if model_field in ("BillAddr", "ShipAddr"):
                if not nested_dict.get("Line1"):
                    continue
                if "Country" not in nested_dict:
                    state = nested_dict.get("CountrySubDivisionCode", "").strip().upper()
                    if state in US_STATES:
                        nested_dict["Country"] = "USA"

            if nested_dict:
                inner[qbo_field] = nested_dict

        inner.update(data)
        return {"Customer": inner}