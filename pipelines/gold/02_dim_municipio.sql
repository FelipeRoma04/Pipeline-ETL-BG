CREATE TABLE IF NOT EXISTS gold.dim_municipio (
  cod_mpio STRING, municipio STRING, cod_depto STRING, departamento STRING
) USING DELTA;
-- TODO: desde silver.divipola
