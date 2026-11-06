# Diccionario de variables (Saber 11, 2019-4 vs 2022-4)

## Confirmacion de esquema

Se valido contra la API oficial de datos.gov.co con el dataset `kgxf-xxbe`:

- Periodos consultados: `20194`, `20224`
- Filas por periodo (conteo real): 1.096.524 y 1.065.888
- Total aproximado: 2.162.412 filas
- Columnas por periodo: 51
- Diferencias de columnas entre periodos: ninguna

La columna del municipio se confirma con el nombre real en la API: `cole_cod_mcpio_ubicacion` (con `mcpio`), no `cole_cod_mpio_ubicacion`.

## Variables clave del dataset

| Variable | 2019-4 | 2022-4 | Nombre en Plata | Tipo | Descripcion | Notas |
|---|---|---|---|---|---|---|
| `periodo` | `20194` | `20224` | `periodo` | string | Periodo de aplicacion del examen | Formato real usado por la API |
| `estu_consecutivo` | presente | presente | `estu_consecutivo` | string | Identificador unico del estudiante | Clave de persona; no se usa como feature |
| `punt_global` | presente | presente | `punt_global` | numeric | Puntaje global del estudiante | Meta objetivo para regresion |
| `cole_cod_dane_establecimiento` | presente | presente | `cole_cod_dane_establecimiento` | string | Codigo DANE del establecimiento | Clave de colegio |
| `cole_naturaleza` | presente | presente | `cole_naturaleza` | string | Naturaleza del establecimiento | Oficial / no oficial |
| `cole_area_ubicacion` | presente | presente | `cole_area_ubicacion` | string | Urbana/Rural | Contexto geografico |
| `cole_jornada` | presente | presente | `cole_jornada` | string | Jornada del colegio | Mañana / tarde / completa |
| `cole_cod_depto_ubicacion` | presente | presente | `cole_cod_depto_ubicacion` | string | Codigo DANE del departamento | Geografia |
| `cole_cod_mcpio_ubicacion` | presente | presente | `cole_cod_mcpio_ubicacion` | string | Codigo DANE del municipio | Correccion del nombre real |
| `fami_estratovivienda` | presente | presente | `fami_estratovivienda` | string | Estrato socioeconomico | Variable de contexto |
| `fami_educacionpadre` | presente | presente | `fami_educacionpadre` | string | Nivel de educacion del padre | Contexto familiar |
| `fami_educacionmadre` | presente | presente | `fami_educacionmadre` | string | Nivel de educacion de la madre | Contexto familiar |
| `fami_tieneinternet` | presente | presente | `fami_tieneinternet` | string | Acceso a internet en el hogar | Contexto digital |
| `fami_tienecomputador` | presente | presente | `fami_tienecomputador` | string | Tiene computador en el hogar | Contexto digital |
| `cole_cod_depto_ubicacion` | presente | presente | `cole_cod_depto_ubicacion` | string | Codigo departamento | Geografia |
| `cole_mcpio_ubicacion` | presente | presente | `cole_mcpio_ubicacion` | string | Nombre del municipio | Metadata geografica |

## Diferencias observadas entre 2019 y 2022

Se compararon columnas reales en la API y el resultado fue:

- No hay columnas nuevas o faltantes entre 2019 y 2022.
- El esquema es consistente para ambos periodos: 51 columnas exactas, mismas claves.
- El cambio real no es estructural sino de distribucion y contenido dentro de las mismas variables.
- La unica correccion necesaria en el contrato fue el nombre real de la columna de municipio:
  - Antes: `cole_cod_mpio_ubicacion`
  - Correcto: `cole_cod_mcpio_ubicacion`

## Observaciones para el pipeline

1. El campo `periodo` esta expresado como texto en la API, con valores `20194` y `20224`.
2. La capa Bronce debe conservar el esquema crudo tal cual llega, añadiendo `_periodo`, `_ingest_ts` y `_fuente`.
3. La capa Plata debe normalizar y tipificar las variables de contexto para construir el indice socioeconomico sin incluir puntajes de areas.
4. La capa ML debe excluir variables que identifiquen directamente al colegio o el puntaje, y mantener el split agrupado por colegio.

Fuentes de metadatos: portal DataIcfes (tipado/casteo), DIVIPOLA DANE (geografia).
