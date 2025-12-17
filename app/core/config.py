# app/core/config.py
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    QBO_CLIENT_ID: str
    QBO_CLIENT_SECRET: str
    QBO_REDIRECT_URI: str
    QBO_ENVIRONMENT: str = "sandbox"
    NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: str
    CLERK_SECRET_KEY: str

    CELERY_BROKER_URL: str 
    CELERY_RESULT_BACKEND: str 

settings = Settings()