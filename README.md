# Saber 11: Pipeline Medallon (PySpark) + ML + Redes Neuronales + LLM

Proyecto de Big Data - Fundacion Universitaria Konrad Lorenz.

## Inicio rapido
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # completar SABER11_DATASET_ID y demas
python main.py --layer bronze
```

## Como trabajar con GitHub Copilot
1. Abre el repo en VS Code (Copilot lee `.github/copilot-instructions.md` automaticamente).
2. En Copilot Chat (modo Agent) ejecuta los prompts de `.github/prompts/` en orden: `/01_bronze`, `/02_silver`, ...
3. Revisa cada resultado, corre `pytest` y marca el checklist de `docs/PLAN.md`.

Estructura y decisiones: ver `docs/PLAN.md`.
