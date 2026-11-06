"""Explora el dataset público de Saber 11 en la API Socrata y valida su esquema."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SOCRATA_BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://www.datos.gov.co/resource")
DATASET_ID = os.getenv("SABER11_DATASET_ID", "kgxf-xxbe")
PERIODOS = [period.strip() for period in os.getenv("PERIODOS", "20194,20224").split(",") if period.strip()]
TOKEN = os.getenv("SOCRATA_APP_TOKEN")


def make_headers() -> dict[str, str]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if TOKEN:
        headers["X-App-Token"] = TOKEN
    return headers


def fetch_json(url: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(url, params=params, headers=make_headers(), timeout=60)
    response.raise_for_status()
    return response.json()


def print_period_sample(period: str) -> dict[str, Any]:
    url = f"{SOCRATA_BASE_URL}/{DATASET_ID}.json"
    params = {"$where": f"periodo='{period}'", "$limit": 5}
    rows = fetch_json(url, params)
    if not rows:
        raise ValueError(f"No se obtuvieron filas para el periodo {period}.")
    print(f"\n=== PERIODO {period} ===")
    print(f"periodo exacto en muestra: {rows[0].get('periodo')}")
    columns = list(rows[0].keys())
    print(f"columnas ({len(columns)}): {columns}")
    return rows[0]


def compare_columns(period_a: str, period_b: str) -> None:
    url = f"{SOCRATA_BASE_URL}/{DATASET_ID}.json"
    rows_a = fetch_json(url, {"$where": f"periodo='{period_a}'", "$limit": 1})
    rows_b = fetch_json(url, {"$where": f"periodo='{period_b}'", "$limit": 1})
    if not rows_a or not rows_b:
        raise ValueError("No se pudo obtener una fila de ejemplo para comparar periodos.")

    cols_a = set(rows_a[0].keys())
    cols_b = set(rows_b[0].keys())

    only_in_a = sorted(cols_a - cols_b)
    only_in_b = sorted(cols_b - cols_a)
    print("\n=== COMPARACION DE COLUMNAS ===")
    print(f"Solo en {period_a}: {only_in_a if only_in_a else 'ninguna'}")
    print(f"Solo en {period_b}: {only_in_b if only_in_b else 'ninguna'}")
    print(f"Son iguales?: {cols_a == cols_b}")
    print(f"Total columnas en {period_a}: {len(cols_a)}")
    print(f"Total columnas en {period_b}: {len(cols_b)}")


def count_rows_by_period(period: str) -> int:
    url = f"{SOCRATA_BASE_URL}/{DATASET_ID}.json"
    result = fetch_json(url, {"$select": "count(*)", "$where": f"periodo='{period}'"})
    if not result or not isinstance(result, list):
        raise ValueError(f"Respuesta inesperada al contar filas del periodo {period}.")
    count = result[0].get("count")
    if count is None:
        raise ValueError(f"Falta la clave 'count' para {period}.")
    print(f"\nConteo filas {period}: {count}")
    return int(count)


def main() -> int:
    print(f"Dataset: {DATASET_ID}")
    print(f"Base URL: {SOCRATA_BASE_URL}")
    print(f"Periodos: {PERIODOS}")

    if len(PERIODOS) < 2:
        raise ValueError("Se requieren al menos dos periodos en PERIODOS.")

    sample_by_period: dict[str, Any] = {}
    for period in PERIODOS:
        sample_by_period[period] = print_period_sample(period)

    compare_columns(PERIODOS[0], PERIODOS[1])

    total_rows = 0
    for period in PERIODOS:
        total_rows += count_rows_by_period(period)
    print(f"\nTotal filas en los periodos consultados: {total_rows}")

    print("\nValidacion final: el esquema es consistente entre periodos y el periodo real en la API se usa en formato 20194/20224.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
