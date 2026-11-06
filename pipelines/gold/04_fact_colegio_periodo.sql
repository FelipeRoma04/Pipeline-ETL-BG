CREATE TABLE IF NOT EXISTS gold.fact_colegio_periodo (
  colegio_id BIGINT, periodo_id INT, n_estudiantes INT,
  pct_estrato_12 DOUBLE, pct_sin_internet DOUBLE, pct_sin_computador DOUBLE,
  pct_padres_baja_escolaridad DOUBLE, indice_socioeconomico DOUBLE,
  puntaje_real DOUBLE, puntaje_esperado DOUBLE, residuo DOUBLE,
  residuo_ajustado DOUBLE, z_score DOUBLE,
  categoria STRING,   -- prioritario | esperado | modelo
  segmento_id INT
) USING DELTA;
