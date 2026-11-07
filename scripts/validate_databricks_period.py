"""Script orientado a Databricks para validar 1 periodo real del dataset Saber 11."""

from __future__ import annotations

import os
import subprocess
import sys


def _read_secret(scope: str, key: str) -> str:
    try:
        # Databricks: dbutils estara disponible en un notebook; si no, se lee desde entorno.
        import dbutils  # type: ignore

        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        return os.getenv(key.upper(), os.getenv("SOCRATA_APP_TOKEN", ""))


def main() -> int:
    dataset_id = os.getenv("SABER11_DATASET_ID", "")
    if not dataset_id:
        raise SystemExit("Falta SABER11_DATASET_ID. Configural el dataset real antes de ejecutar la validacion.")

    token = _read_secret("saber11", "socrata_token")
    if not token:
        raise SystemExit("Falta SOCRATA_APP_TOKEN en secrets de Databricks o en las variables de entorno.")

    os.environ["ENVIRONMENT"] = "databricks"
    os.environ["USE_FALLBACK"] = "false"
    os.environ["SOCRATA_APP_TOKEN"] = token

    cmd = [
        sys.executable,
        "main.py",
        "--environment",
        "databricks",
        "--no-fallback",
        "--layer",
        "bronze",
        "--periodos",
        "20194",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
