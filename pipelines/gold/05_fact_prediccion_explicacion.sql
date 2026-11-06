CREATE TABLE IF NOT EXISTS gold.fact_prediccion (
  colegio_id BIGINT, periodo_id INT, modelo STRING, version STRING,
  valor_predicho DOUBLE, ts TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS gold.fact_explicacion (
  colegio_id BIGINT, periodo_id INT, texto STRING, modelo_llm STRING,
  prompt_version STRING, ts TIMESTAMP
) USING DELTA;
