"""Mapeos de columnas crudas -> columnas de Plata, por periodo."""

MAPPING_SABER11_2019 = {
    "periodo": "periodo",
    "estu_consecutivo": "estu_consecutivo",
    "cole_cod_dane_establecimiento": "colegio_id",
    "cole_naturaleza": "cole_naturaleza",
    "cole_area_ubicacion": "cole_area_ubicacion",
    "cole_jornada": "cole_jornada",
    "cole_cod_depto_ubicacion": "cod_depto",
    "cole_cod_mcpio_ubicacion": "cod_mpio",
    "fami_estratovivienda": "fami_estratovivienda",
    "fami_educacionpadre": "fami_educacionpadre",
    "fami_educacionmadre": "fami_educacionmadre",
    "fami_tieneinternet": "fami_tieneinternet",
    "fami_tienecomputador": "fami_tienecomputador",
    "punt_global": "punt_global",
}

MAPPING_SABER11_2022 = {
    "periodo": "periodo",
    "estu_consecutivo": "estu_consecutivo",
    "cole_cod_dane_establecimiento": "colegio_id",
    "cole_naturaleza": "cole_naturaleza",
    "cole_area_ubicacion": "cole_area_ubicacion",
    "cole_jornada": "cole_jornada",
    "cole_cod_depto_ubicacion": "cod_depto",
    "cole_cod_mcpio_ubicacion": "cod_mpio",
    "fami_estratovivienda": "fami_estratovivienda",
    "fami_educacionpadre": "fami_educacionpadre",
    "fami_educacionmadre": "fami_educacionmadre",
    "fami_tieneinternet": "fami_tieneinternet",
    "fami_tienecomputador": "fami_tienecomputador",
    "punt_global": "punt_global",
}

COMMON_SABER11_COLUMNS = list(MAPPING_SABER11_2019.keys())
