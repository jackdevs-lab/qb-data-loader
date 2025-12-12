# app/models/db.py
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
import os

class User(Model):
    id = fields.IntField(pk=True)
    email = fields.CharField(max_length=255, unique=True)
    hashed_password = fields.CharField(max_length=255)
    qbo_realm_id = fields.CharField(max_length=50, null=True)
    qbo_access_token = fields.TextField(null=True)
    qbo_refresh_token = fields.TextField(null=True)
    qbo_expires_at = fields.BigIntField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

class MappingTemplate(Model):
    id = fields.IntField(pk=True)
    user = fields.ForeignKeyField("models.User", related_name="mappings")
    name = fields.CharField(max_length=100)
    object_type = fields.CharField(max_length=50)
    mapping = fields.JSONField()  # {"CSV Header": "QBO.Field.Name"}
    created_at = fields.DatetimeField(auto_now_add=True)

class Job(Model):
    id = fields.IntField(pk=True)
    user = fields.ForeignKeyField("models.User", related_name="jobs")
    object_type = fields.CharField(max_length=50)
    status = fields.CharField(max_length=20, default="queued")  # queued → parsing → validating → importing → done|failed
    meta = fields.JSONField(default=dict)
    created_at = fields.DatetimeField(auto_now_add=True)

class JobRow(Model):
    id = fields.IntField(pk=True)
    job = fields.ForeignKeyField("models.Job", related_name="rows")
    row_number = fields.IntField()
    status = fields.CharField(max_length=20, default="pending")
    error = fields.TextField(null=True)
    raw_data = fields.JSONField()
    payload = fields.JSONField(null=True)  # final QBO-ready dict after mapping+validation

TORTOISE_ORM = {
    "connections": {
        "default": os.environ.get("DATABASE_URL", "sqlite://db.sqlite3")
    },
    "apps": {
        "models": {
            "models": ["app.models.db", "aerich.models"],
            "default_connection": "default",
        }
    },
}