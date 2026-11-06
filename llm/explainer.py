from __future__ import annotations

import json
from typing import Any


def explain_prediction(summary: dict[str, Any]) -> dict[str, Any]:
    """Genera una respuesta estructurada sin calcular estadisticas; solo redacta valores ya medidos."""
    models = summary.get("modelos", {})
    lr = models.get("linear_regression", {})
    mlp = models.get("mlp", {})

    payload = {
        "modelo_base": "Regresion lineal con contexto socioeconomico",
        "modelo_referencia": "MLP",
        "observacion": "El modelo explica el puntaje esperado usando variables de contexto y no atributos de identidad del colegio.",
        "metricas": {
            "linear_regression": {
                "rmse": lr.get("rmse"),
                "mae": lr.get("mae"),
                "r2": lr.get("r2"),
            },
            "mlp": {
                "rmse": mlp.get("rmse"),
                "mae": mlp.get("mae"),
                "r2": mlp.get("r2"),
            },
        },
        "recomendacion": "Usar la linea base como comparacion y revisar si la diferencia del MLP justifica complejidad adicional.",
        "prompt_version": "llm_v1",
    }
    return payload


def render_explanation_json(summary: dict[str, Any]) -> str:
    """Serializa la explicacion en JSON para persistencia."""
    return json.dumps(explain_prediction(summary), ensure_ascii=False, indent=2)
