"""Runner completo: Bronce -> Plata -> Oro. TODO: implementar con Copilot (ver .github/prompts)."""
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    p.add_argument("--periodos", nargs="*", default=None)
    args = p.parse_args()
    raise SystemExit("TODO: implementar orquestacion")
