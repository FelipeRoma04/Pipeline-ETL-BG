"""Configuracion central. Lee variables de entorno (.env) y detecta Databricks."""
import os
import shutil
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # En Databricks python-dotenv no esta instalado; no hace falta
    pass


def ensure_hadoop_windows_env() -> str:
    """Configura una ruta local de Hadoop para Spark en Windows cuando falta winutils."""
    if os.name != "nt":
        return os.environ.get("HADOOP_HOME", "")

    hadoop_home = Path(os.environ.get("HADOOP_HOME") or r"C:\hadoop")
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    winutils = bin_dir / "winutils.exe"

    if not winutils.exists():
        url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.4.2/bin/winutils.exe"
        try:
            with urllib.request.urlopen(url, timeout=30) as response, open(winutils, "wb") as target:
                shutil.copyfileobj(response, target)
        except Exception:
            pass

    if winutils.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        path_value = os.environ.get("PATH", "")
        if str(bin_dir) not in path_value.split(";"):
            os.environ["PATH"] = str(bin_dir) + (";" if path_value else "") + path_value
        return str(hadoop_home)

    return str(hadoop_home)


ensure_hadoop_windows_env()


def _detect_environment() -> str:
    """Detecta si la ejecucion corre dentro de Databricks (notebook o job)."""
    override = os.getenv("ENVIRONMENT", "").strip().lower()
    if override:
        return override
    if os.getenv("DATABRICKS_RUNTIME_VERSION") or os.getenv("DATABRICKS_HOST"):
        return "databricks"
    return "local"


ENVIRONMENT = _detect_environment()
USE_FALLBACK = os.getenv("USE_FALLBACK", "true" if ENVIRONMENT == "local" else "false").strip().lower() in {"1", "true", "yes", "on"}

if ENVIRONMENT == "databricks":
    # Ruta duradera en workspace files, al lado del repo (HOME apunta a un dir efimero)
    _DEFAULT_DATA_ROOT = str(Path(__file__).resolve().parents[2] / "saber11_data")
else:
    _DEFAULT_DATA_ROOT = "./data"
DATA_ROOT = Path(os.getenv("DATA_ROOT", _DEFAULT_DATA_ROOT))
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"
QUARANTINE_PATH = DATA_ROOT / "quarantine"

# Catalogo de Unity Catalog del workspace; cada capa usa su propio esquema:
# workspace.bronze.*, workspace.silver.*, workspace.gold.*
UNITY_CATALOG = os.getenv("UNITY_CATALOG", "workspace")

SOCRATA_BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://www.datos.gov.co/resource")
SABER11_DATASET_ID = (os.getenv("SABER11_DATASET_ID", "") or "kgxf-xxbe").strip()
SOCRATA_APP_TOKEN = (os.getenv("SOCRATA_APP_TOKEN", "") or "").strip()
PERIODOS = [p.strip() for p in os.getenv("PERIODOS", "20194,20224").split(",") if p.strip()]


def is_databricks() -> bool:
    """Indica si la ejecucion actual corre sobre Databricks/Unity Catalog."""
    return ENVIRONMENT == "databricks" or bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))


def should_use_fallback() -> bool:
    """Devuelve True cuando la ejecucion debe quedarse en el dataset local de respaldo."""
    return USE_FALLBACK or not SABER11_DATASET_ID


def get_uc_table_name(layer: str, table_name: str) -> str:
    """Genera el nombre completo catalog.<layer>.<table> para Unity Catalog."""
    return f"{UNITY_CATALOG}.{layer}.{table_name}"
