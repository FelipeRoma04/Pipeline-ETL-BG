CREATE TABLE IF NOT EXISTS gold.dim_colegio (
  colegio_id BIGINT, cole_cod_dane STRING, naturaleza STRING, area_ubicacion STRING,
  jornada STRING, cod_mpio STRING, cod_depto STRING
) USING DELTA;
-- TODO: INSERT OVERWRITE desde silver.colegios (idempotente)
