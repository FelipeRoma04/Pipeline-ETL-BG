from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, silhouette_score
from sklearn.neural_network import MLPRegressor

from config.settings import GOLD_PATH

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = [
    "fami_estratovivienda",
    "fami_tieneinternet",
    "fami_tienecomputador",
    "indice_socioeconomico",
]


def _build_demo_rows() -> list[dict[str, Any]]:
    """Genera un conjunto sintético por colegio para validar split agrupado y residuos."""
    rows: list[dict[str, Any]] = []
    for colegio_idx in range(1, 7):
        periodo = "20194" if colegio_idx <= 3 else "20224"
        for estudiante_idx in range(12):
            estrato = ((colegio_idx + estudiante_idx) % 6) + 1
            internet = 1 if (colegio_idx + estudiante_idx) % 3 == 0 else 0
            computador = 1 if (colegio_idx + estudiante_idx) % 2 == 0 else 0
            indice = 0.2 + (estrato * 0.08) + (internet * 0.2) + (computador * 0.2)
            base = 210 + colegio_idx * 18 + estrato * 16 + internet * 26 + computador * 19
            punt_global = base + (estudiante_idx % 8) * 5
            rows.append(
                {
                    "colegio_id": f"C{colegio_idx:03d}",
                    "periodo": periodo,
                    "fami_estratovivienda": estrato,
                    "fami_tieneinternet": internet,
                    "fami_tienecomputador": computador,
                    "indice_socioeconomico": indice,
                    "punt_global": punt_global,
                }
            )
    return rows


def _prepare_features(rows: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
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
    colegios = np.asarray([row.get("colegio_id", "unknown") for row in rows], dtype=object)
    return X, y, colegios


def _grouped_split(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], list[str]]:
    colegios = sorted({row["colegio_id"] for row in rows})
    train_colegios = colegios[:4]
    test_colegios = colegios[4:]
    train_rows = [row for row in rows if row["colegio_id"] in train_colegios]
    test_rows = [row for row in rows if row["colegio_id"] in test_colegios]
    return train_rows, test_rows, train_colegios, test_colegios


def _residual_summary(colegio_ids: list[str], true_values: np.ndarray, pred_values: np.ndarray) -> dict[str, Any]:
    residuals = np.asarray(true_values - pred_values, dtype=float)
    by_colegio: dict[str, float] = {}
    for colegio_id in sorted(set(colegio_ids)):
        mask = np.asarray([item == colegio_id for item in colegio_ids], dtype=bool)
        by_colegio[colegio_id] = float(np.mean(residuals[mask]))

    z_scores = np.asarray([by_colegio[col] for col in sorted(by_colegio)], dtype=float)
    mean_z = float(np.mean(z_scores)) if z_scores.size else 0.0
    std_z = float(np.std(z_scores)) if z_scores.size else 0.0
    if std_z == 0:
        std_z = 1.0
    z_by_colegio = {col: float((by_colegio[col] - mean_z) / std_z) for col in sorted(by_colegio)}
    prioritarios = [col for col, z in z_by_colegio.items() if z <= -1.5]
    modelo = [col for col, z in z_by_colegio.items() if z >= 1.5]
    return {
        "promedio_residual": float(np.mean(residuals)),
        "residuo_por_colegio": by_colegio,
        "zscore_por_colegio": z_by_colegio,
        "colegios_prioritarios": prioritarios,
        "colegios_modelo": modelo,
    }


def _metrics_for_model(model: Any, X_train: np.ndarray, y_train: np.ndarray, X_test: np.ndarray, y_test: np.ndarray, colegio_test: list[str]) -> dict[str, Any]:
    pred_train = model.predict(X_train)
    pred_test = model.predict(X_test)
    stats = {
        "train_rmse": float(np.sqrt(mean_squared_error(y_train, pred_train))),
        "train_mae": float(mean_absolute_error(y_train, pred_train)),
        "train_r2": float(r2_score(y_train, pred_train)),
        "test_rmse": float(np.sqrt(mean_squared_error(y_test, pred_test))),
        "test_mae": float(mean_absolute_error(y_test, pred_test)),
        "test_r2": float(r2_score(y_test, pred_test)),
    }
    stats["residual"] = _residual_summary(colegio_test, y_test, pred_test)
    return stats


def run_ml() -> dict[str, Any]:
    """Entrena modelos de referencia con split agrupado por colegio y metricas de residuo."""
    rows = _build_demo_rows()
    train_rows, test_rows, train_colegios, test_colegios = _grouped_split(rows)

    X_train, y_train, _ = _prepare_features(train_rows)
    X_test, y_test, colegios_test = _prepare_features(test_rows)

    models = {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(n_estimators=150, random_state=42),
        "mlp": MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=800, random_state=42),
    }

    metrics: dict[str, dict[str, Any]] = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        metrics[name] = _metrics_for_model(model, X_train, y_train, X_test, y_test, list(colegios_test))

    X_all, y_all, colegios_all = _prepare_features(rows)
    kmeans = KMeans(n_clusters=3, n_init=10, random_state=42)
    kmeans.fit(X_all)
    silhouette = silhouette_score(X_all, kmeans.labels_) if len(np.unique(kmeans.labels_)) > 1 else 0.0
    metrics["kmeans"] = {
        "silhouette_score": float(silhouette),
        "cluster_size": {int(k): int(np.sum(kmeans.labels_ == k)) for k in range(len(np.unique(kmeans.labels_)))},
        "centroid_distance_proxy": float(np.mean(np.linalg.norm(X_all - kmeans.cluster_centers_[kmeans.labels_], axis=1))),
    }

    linear_pred = models["linear_regression"].predict(X_test)
    residual_sets = [
        metrics[name]["residual"]
        for name in metrics
        if isinstance(metrics[name], dict) and "residual" in metrics[name]
    ]
    all_prioritarios = sorted({value for residual in residual_sets for value in residual["colegios_prioritarios"]})
    all_modelos = sorted({value for residual in residual_sets for value in residual["colegios_modelo"]})

    summary = {
        "version": "ml_v2_grouped_split",
        "num_registros": len(rows),
        "split": {
            "strategy": "agrupado_por_colegio",
            "train_colegios": train_colegios,
            "test_colegios": test_colegios,
            "train_rows": len(train_rows),
            "test_rows": len(test_rows),
        },
        "features": FEATURE_COLUMNS,
        "modelos": metrics,
        "reglas": {
            "split": "agrupado por colegio para evitar fuga de datos",
            "features": "solo contexto (sin identificar el colegio ni puntajes de areas)",
            "residuo": "promedio por colegio y z-score de residuos para detectar colegios prioritarios o modelo",
        },
        "residual_summary": {
            "global_mean": float(np.mean(y_test - linear_pred)),
            "colegios_prioritarios": all_prioritarios,
            "colegios_modelo": all_modelos,
        },
    }

    output_path = GOLD_PATH / "ml_summary.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Resumen ML actualizado en %s", output_path)
    return summary
