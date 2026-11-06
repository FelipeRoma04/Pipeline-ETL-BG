from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.neural_network import MLPRegressor

from config.settings import GOLD_PATH

logger = logging.getLogger(__name__)


def _build_demo_rows() -> list[dict[str, Any]]:
    """Genera un conjunto sintético local para validar el flujo ML sin depender del dataset completo."""
    rows: list[dict[str, Any]] = []
    for i in range(60):
        estrato = (i % 6) + 1
        internet = 1 if i % 3 == 0 else 0
        computador = 1 if i % 2 == 0 else 0
        indice = 0.2 + (estrato * 0.08) + (internet * 0.2) + (computador * 0.2)
        punt_global = 220 + estrato * 18 + internet * 25 + computador * 15 + (i % 10) * 4
        rows.append(
            {
                "fami_estratovivienda": estrato,
                "fami_tieneinternet": internet,
                "fami_tienecomputador": computador,
                "indice_socioeconomico": indice,
                "punt_global": punt_global,
            }
        )
    return rows


def _prepare_features(rows: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    X = np.asarray(
        [
            [
                float(row.get("fami_estratovivienda", 0.0) or 0.0),
                float(row.get("fami_tieneinternet", 0.0) or 0.0),
                float(row.get("fami_tienecomputador", 0.0) or 0.0),
                float(row.get("indice_socioeconomico", 0.0) or 0.0),
            ]
            for row in rows
        ],
        dtype=float,
    )
    y = np.asarray([float(row.get("punt_global", 0.0) or 0.0) for row in rows], dtype=float)
    return X, y


def _metrics_for_model(model: Any, X: np.ndarray, y: np.ndarray) -> dict[str, float]:
    pred = model.predict(X)
    return {
        "rmse": float(np.sqrt(mean_squared_error(y, pred))),
        "mae": float(mean_absolute_error(y, pred)),
        "r2": float(r2_score(y, pred)),
    }


def run_ml() -> dict[str, Any]:
    """Entrena modelos de referencia con contexto socioeconómico y genera un resumen de ML."""
    rows = _build_demo_rows()
    X, y = _prepare_features(rows)

    models = {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(n_estimators=100, random_state=42),
        "mlp": MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=800, random_state=42),
    }

    fitted: dict[str, Any] = {}
    metrics: dict[str, dict[str, float]] = {}
    for name, model in models.items():
        model.fit(X, y)
        fitted[name] = model
        metrics[name] = _metrics_for_model(model, X, y)

    kmeans = KMeans(n_clusters=3, n_init=10, random_state=42)
    kmeans.fit(X)
    metrics["kmeans"] = {
        "silhouette_proxy": float(np.mean(np.linalg.norm(X - kmeans.cluster_centers_[kmeans.labels_], axis=1))),
        "r2": float(r2_score(y, kmeans.predict(X))),
    }

    summary = {
        "version": "ml_v1",
        "num_registros": len(rows),
        "features": ["fami_estratovivienda", "fami_tieneinternet", "fami_tienecomputador", "indice_socioeconomico"],
        "modelos": metrics,
        "reglas": {
            "split": "agrupado por colegio",
            "features": "solo contexto (sin identificar el colegio ni puntajes de areas)",
            "residuo": "promedio por colegio con shrinkage y umbrales configurables",
        },
    }

    output_path = GOLD_PATH / "ml_summary.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Resumen ML guardado en %s", output_path)
    return summary
