from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from config.settings import GOLD_PATH

logger = logging.getLogger(__name__)


def run_llm(metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    """Genera una explicacion estructurada a partir de metricas ya calculadas por ML."""
    if metrics is None:
        metrics_path = GOLD_PATH / "ml_summary.json"
        if not metrics_path.exists():
            raise FileNotFoundError("No existe el resumen ML; primero ejecute la capa ML.")
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    modelos = metrics.get("modelos", {})
    payload = {
        "prompt_version": "llm_v1",
        "modelo_base": "regresion lineal sobre contexto socioeconomico",
        "modelo_referencia": "random_forest + MLP",
        "observaciones": [
            "El LLM no calcula metricas; solo resume resultados ya medidos por ML.",
            "Las features usadas son solo contexto del estudiante y del hogar.",
            "La comparacion incluye regresion lineal, Random Forest y MLP para ser honesta.",
        ],
        "metricas": {
            "linear_regression": modelos.get("linear_regression", {}),
            "random_forest": modelos.get("random_forest", {}),
            "mlp": modelos.get("mlp", {}),
        },
        "recomendacion": "Si el MLP supera levemente a arboles, conviene mantener la simplicidad y usar la linea base; de lo contrario, priorizar modelos explicables.",
    }

    output_path = GOLD_PATH / "fact_explicacion.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Explicacion LLM guardada en %s", output_path)
    return payload
