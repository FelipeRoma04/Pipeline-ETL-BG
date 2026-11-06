CREATE TABLE IF NOT EXISTS gold.dim_periodo (periodo_id INT, periodo STRING, anio INT) USING DELTA;
CREATE TABLE IF NOT EXISTS gold.dim_segmento (segmento_id INT, nombre STRING, descripcion STRING) USING DELTA;
