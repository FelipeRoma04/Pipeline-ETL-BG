"""Runner completo del pipeline Saber 11: Bronce -> Plata -> Oro -> ML -> LLM."""
import argparse
import logging
import os

from llm.run_llm import run_llm
from ml.run_ml import run_ml
from pipelines.bronze.run_bronze import run_bronze
from pipelines.gold.run_gold import run_gold
from pipelines.silver.run_silver import run_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


def configure_runtime(environment: str, force_fallback: bool, no_fallback: bool) -> None:
    """Configura el entorno de ejecucion para soporte local y futuro Databricks."""
    os.environ["ENVIRONMENT"] = environment
    if force_fallback:
        os.environ["USE_FALLBACK"] = "true"
    elif no_fallback:
        os.environ["USE_FALLBACK"] = "false"
    else:
        os.environ["USE_FALLBACK"] = "true" if environment == "local" else "false"


def main() -> int:
    parser = argparse.ArgumentParser(description="Orquesta el pipeline Saber 11 por capas.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "ml", "llm", "all"], default="all")
    parser.add_argument("--periodos", nargs="*", default=None, help="Periodos a procesar, por ejemplo 20194 20224")
    parser.add_argument("--environment", choices=["local", "databricks"], default="local", help="Ambiente de ejecucion.")
    parser.add_argument("--force-fallback", action="store_true", help="Fuerza ejecucion local con dataset sintético.")
    parser.add_argument("--no-fallback", action="store_true", help="Desactiva el fallback y requiere dataset real.")
    args = parser.parse_args()

    configure_runtime(args.environment, args.force_fallback, args.no_fallback)
    logger.info("Ambiente activo: %s | USE_FALLBACK=%s", args.environment, os.getenv("USE_FALLBACK", "true"))

    if args.layer in {"bronze", "all"}:
        logger.info("Ejecutando capa Bronce")
        run_bronze(periodos=args.periodos)

    if args.layer in {"silver", "all"}:
        logger.info("Ejecutando capa Plata")
        run_silver(periodos=args.periodos)

    if args.layer in {"gold", "all"}:
        logger.info("Ejecutando capa Oro")
        run_gold()

    if args.layer in {"ml", "all"}:
        logger.info("Ejecutando capa ML")
        run_ml()

    if args.layer in {"llm", "all"}:
        logger.info("Ejecutando capa LLM")
        run_llm()

    logger.info("Pipeline finalizado para la capa %s", args.layer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
