from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.neural_network import MLPRegressor


def build_context_features(rows: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    """Construye X e y a partir de variables de contexto del estudiante."""
    X: list[list[float]] = []
    y: list[float] = []
    for row in rows:
        x_row = [
            float(row.get("fami_estratovivienda", 0.0) or 0.0),
            float(row.get("fami_tieneinternet", 0.0) or 0.0),
            float(row.get("fami_tienecomputador", 0.0) or 0.0),
            float(row.get("indice_socioeconomico", 0.0) or 0.0),
        ]
        X.append(x_row)
        y.append(float(row.get("punt_global", 0.0) or 0.0))
    return X, y


def evaluate_models(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Evalua un baseline lineal y un MLP como comparacion simple."""
    X, y = build_context_features(rows)
    X_arr = np.asarray(X, dtype=float)
    y_arr = np.asarray(y, dtype=float)

    lr = LinearRegression()
    lr.fit(X_arr, y_arr)
    lr_pred = lr.predict(X_arr)

    mlp = MLPRegressor(hidden_layer_sizes=(16, 8), max_iter=500, random_state=42)
    mlp.fit(X_arr, y_arr)
    mlp_pred = mlp.predict(X_arr)

    metrics = {
        "linear_regression": {
            "rmse": float(np.sqrt(mean_squared_error(y_arr, lr_pred))),
            "mae": float(mean_absolute_error(y_arr, lr_pred)),
            "r2": float(r2_score(y_arr, lr_pred)),
        },
        "mlp": {
            "rmse": float(np.sqrt(mean_squared_error(y_arr, mlp_pred))),
            "mae": float(mean_absolute_error(y_arr, mlp_pred)),
            "r2": float(r2_score(y_arr, mlp_pred)),
        },
    }
    return metrics


def build_prediction_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Genera un resumen de entrenamiento con modelos y metrica simple."""
    metrics = evaluate_models(rows)
    return {
        "modelos": metrics,
        "num_registros": len(rows),
        "nota": "Este es un baseline local para validacion del flujo; el proyecto exige split por colegio y comparacion honesta con modelos no lineales.",
    }
