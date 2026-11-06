"""Configuracion central. Lee variables de entorno (.env)."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data"))
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"
QUARANTINE_PATH = DATA_ROOT / "quarantine"

SOCRATA_BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://www.datos.gov.co/resource")
SABER11_DATASET_ID = os.getenv("SABER11_DATASET_ID", "")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
PERIODOS = [p.strip() for p in os.getenv("PERIODOS", "20194,20224").split(",")]
