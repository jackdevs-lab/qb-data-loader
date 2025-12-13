# app/schemas/validators.py
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional, Dict, Any


class CustomerRow(BaseModel):
    DisplayName: str = Field(..., min_length=1, max_length=500)
    PrimaryEmailAddr: Optional[EmailStr] = None
    PrimaryPhone: Optional[str] = None
    BillAddr_Line1: Optional[str] = None
    BillAddr_City: Optional[str] = None
    BillAddr_CountrySubDivisionCode: Optional[str] = None
    BillAddr_PostalCode: Optional[str] = None

    model_config = {"extra": "allow"}

    # Clean up empty strings → None early (helps validation and avoids empty objects)
    @field_validator(
        "PrimaryEmailAddr",
        "PrimaryPhone",
        "BillAddr_Line1",
        "BillAddr_City",
        "BillAddr_CountrySubDivisionCode",
        "BillAddr_PostalCode",
        mode="before",
    )
    @classmethod
    def empty_str_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        # Do NOT pass exclude_unset/exclude_none here — let the caller control it
        # This avoids duplicate argument errors when caller passes them
        data = super().model_dump(**kwargs)

        result: Dict[str, Any] = {}

        # Required
        result["DisplayName"] = data["DisplayName"]

        # Email – only if present (EmailStr ensures it's valid)
        if data.get("PrimaryEmailAddr"):
            result["PrimaryEmailAddr"] = {"Address": data["PrimaryEmailAddr"]}

        # Phone – only if non-empty after strip
        if data.get("PrimaryPhone"):
            phone = data["PrimaryPhone"].strip()
            if phone:
                result["PrimaryPhone"] = {"FreeFormNumber": phone}

        # Billing Address – build only if any part exists
        bill_addr: Dict[str, str] = {}
        addr_mapping = {
            "Line1": "BillAddr_Line1",
            "City": "BillAddr_City",
            "CountrySubDivisionCode": "BillAddr_CountrySubDivisionCode",
            "PostalCode": "BillAddr_PostalCode",
        }

        for qbo_key, csv_key in addr_mapping.items():
            value = data.get(csv_key)
            if value:
                cleaned = str(value).strip()
                if cleaned:
                    bill_addr[qbo_key] = cleaned

        if bill_addr:
            result["BillAddr"] = bill_addr

        return result


VALIDATORS = {
    "customer": CustomerRow,
}