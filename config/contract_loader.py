"""Carga contratos de datos YAML por fuente usando rutas absolutas al repo."""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = REPO_ROOT / "config" / "contracts"


def load_contract(name: str) -> dict:
    path = CONTRACTS_DIR / "bronze" / f"{name}.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
