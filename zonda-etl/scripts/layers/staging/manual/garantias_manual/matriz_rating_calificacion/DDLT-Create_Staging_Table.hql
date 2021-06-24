CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.matriz_rating_calificacion
(
      categoria string
    , moodys    string
    , sp        string
    , fitch     string
    , calificacion_empresa  tinyint
    , calificacion_pais     tinyint
)
PARTITIONED BY (partition_date string)
--COMMENT 'Tabla auxiliar Matriz de Calificacion Moodys, S&P y Fitch por Rating'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/matriz_rating_calificacion';