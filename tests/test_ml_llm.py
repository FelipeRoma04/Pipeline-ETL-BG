from __future__ import annotations

from llm.run_llm import run_llm
from ml.run_ml import run_ml


def test_ml_summary_generates_metrics() -> None:
    summary = run_ml()
    assert summary["num_registros"] > 0
    assert "linear_regression" in summary["modelos"]
    assert "random_forest" in summary["modelos"]
    assert "mlp" in summary["modelos"]


def test_llm_summary_is_structured() -> None:
    summary = run_ml()
    payload = run_llm(summary)
    assert payload["prompt_version"] == "llm_v1"
    assert "metricas" in payload
