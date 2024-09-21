import os
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union
# from dotenv import load_dotenv
from core.parser import get_param

# load_dotenv()
class Configs:
    """Configs Class."""
    REGION : Optional[str] = os.getenv("REGION" , "ap-south-1")
    PROJECT_NAME: Optional[str] = os.getenv("PROJECT_NAME" , "ion-middleware")
    PROJECT_VERSION: Optional[str] = os.getenv("PROJECT_VERSION", "1.0.0")
    POSTGRES_USER: Optional[str] = os.environ.get('POSTGRES_USER')
    POSTGRES_HOST: Optional[str] = os.environ.get('POSTGRES_HOST')
    POSTGRES_PORT: Union[str, int, None] = os.environ.get('POSTGRES_PORT')
    POSTGRES_DATABASE: Optional[str] = os.environ.get('POSTGRES_DATABASE')
    BACKEND_CORS_ORIGIN: Optional[str] = ["*"]
    try:
        POSTGRES_PASSWORD : Optional[str] = get_param(REGION,"psql")
    except Exception as e:
        POSTGRES_PASSWORD : str = os.getenv("POSTGRES_PASSWORD")
    DATABASE_URL: str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}"
    SCAN_QUEUE_URL :str = os.getenv("SCAN_QUEUE_URL")
    ES_PORT:str=os.getenv("ES_HOST")
    DEPLOYEMENT_TYPE : str = os.getenv("DEPLOYEMENT_TYPE")
    ENVIRONMENT : str = os.getenv("ENVIRONMENT")
    

# @lru_cache
def get_configs() -> Configs:
    '''Get cached Configs'''
    configs = Configs()
    return configs

