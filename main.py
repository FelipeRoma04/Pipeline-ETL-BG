"""Runner completo del pipeline Saber 11: Bronce -> Plata -> Oro -> ML -> LLM."""
import argparse
import logging

from pipelines.bronze.run_bronze import run_bronze
from pipelines.gold.run_gold import run_gold
from pipelines.silver.run_silver import run_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Orquesta el pipeline Saber 11 por capas.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "ml", "llm", "all"], default="all")
    parser.add_argument("--periodos", nargs="*", default=None, help="Periodos a procesar, por ejemplo 20194 20224")
    args = parser.parse_args()

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
        logger.info("Capa ML no ejecuta un entrenamiento completo sin dataset de referencia; se deja preparada para la validacion posterior.")

    if args.layer in {"llm", "all"}:
        logger.info("Capa LLM lista para generar explicacion JSON a partir de resultados ya calculados al final del pipeline.")

    logger.info("Pipeline finalizado para la capa %s", args.layer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
