"""Configuracion central. Lee variables de entorno (.env)."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "local").strip().lower()
USE_FALLBACK = os.getenv("USE_FALLBACK", "true" if ENVIRONMENT == "local" else "false").strip().lower() in {"1", "true", "yes", "on"}

DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data"))
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"
QUARANTINE_PATH = DATA_ROOT / "quarantine"

UNITY_CATALOG = os.getenv("UNITY_CATALOG", "saber11")
UNITY_SCHEMA = os.getenv("UNITY_SCHEMA", "analytics")

SOCRATA_BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://www.datos.gov.co/resource")
SABER11_DATASET_ID = (os.getenv("SABER11_DATASET_ID", "") or "").strip()
SOCRATA_APP_TOKEN = (os.getenv("SOCRATA_APP_TOKEN", "") or "").strip()
PERIODOS = [p.strip() for p in os.getenv("PERIODOS", "20194,20224").split(",") if p.strip()]


def is_databricks() -> bool:
    """Indica si la ejecucion actual corre sobre Databricks/Unity Catalog."""
    return ENVIRONMENT == "databricks"


def should_use_fallback() -> bool:
    """Devuelve True cuando la ejecucion debe quedarse en el dataset local de respaldo."""
    return USE_FALLBACK or not SABER11_DATASET_ID


def get_uc_table_name(layer: str, table_name: str) -> str:
    """Genera el nombre completo catalog.schema.table para Unity Catalog."""
    return f"{UNITY_CATALOG}.{UNITY_SCHEMA}.{layer}_{table_name}"
