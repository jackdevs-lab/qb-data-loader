import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from app.main import app
from app.schemas.customer import CustomerCanonical

from app.core.auth import get_current_user
from app.models.db import User

client = TestClient(app)

# Bypass auth for all tests
async def mock_get_current_user():
    return AsyncMock(spec=User, id=1, qbo_realm_id="12345")

app.dependency_overrides[get_current_user] = mock_get_current_user

@pytest.fixture
def mock_qbo_client():
    with patch("app.api.customers.get_qbo_client", new_callable=AsyncMock) as mock:
        yield mock

# We no longer need the mock_user fixture in every test but can keep it if needed
# Actually, let's remove it to stay clean.

def test_list_customers(mock_qbo_client):
    mock_client = mock_qbo_client.return_value
    mock_client.get.return_value = AsyncMock(
        json=lambda: {"QueryResponse": {"Customer": [{"Id": "1", "DisplayName": "Test"}]}},
        is_error=False,
        status_code=200
    )
    mock_client.get.return_value.raise_for_status = lambda: None
    
    response = client.get("/api/customers")
    assert response.status_code == 200
    assert "customers" in response.json()
    assert response.json()["customers"][0]["DisplayName"] == "Test"

def test_create_customer_invalid_email():
    # This should fail validation before even hitting the mock QBO client
    payload = {
        "DisplayName": "Bad Email",
        "PrimaryEmailAddr": {"Address": "invalid..email@example.com"}
    }
    response = client.post("/api/customers", json=payload)
    assert response.status_code == 422 # Pydantic validation error

def test_update_customer_sparse(mock_qbo_client):
    mock_client = mock_qbo_client.return_value
    
    # Mock the initial fetch for SyncToken
    mock_client.get.return_value = AsyncMock(
        json=lambda: {"Customer": {"Id": "1", "SyncToken": "5"}},
        is_error=False,
        status_code=200
    )
    mock_client.get.return_value.raise_for_status = lambda: None
    
    # Mock the update call
    mock_client.post.return_value = AsyncMock(
        json=lambda: {"Customer": {"Id": "1", "SyncToken": "6"}},
        is_error=False,
        status_code=200
    )
    
    payload = {"DisplayName": "Updated Name"}
    response = client.put("/api/customers/1", json=payload)
    
    assert response.status_code == 200
    assert response.json()["Customer"]["SyncToken"] == "6"
    assert mock_client.post.called
    # Verify SyncToken was fetched and passed
    args, kwargs = mock_client.post.call_args
    assert kwargs["json"]["SyncToken"] == "5"
    assert kwargs["json"]["sparse"] is True
