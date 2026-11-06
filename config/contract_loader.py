"""Carga contratos de datos YAML por fuente (patron del proyecto de ejemplo)."""
from pathlib import Path
import yaml


def load_contract(name: str) -> dict:
    path = Path(f"config/contracts/bronze/{name}.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
