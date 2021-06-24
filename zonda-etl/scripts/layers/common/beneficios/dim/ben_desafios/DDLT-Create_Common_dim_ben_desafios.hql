
CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_ben_desafios (

cod_ben_desafio string,
ds_ben_nombredesafio string,
ds_ben_descripciondesafio string,
fc_ben_puntosduracion string,
dt_ben_desde string,
dt_ben_hasta string,
cod_ben_renovacion string,
dt_ben_desdeoriginal string,
dt_hastaoriginal string,
fc_ben_puntosencadenado int,
cod_ben_encadenado string,
cod_ben_codigobonus string,
cod_ben_desafiopadre string,
cod_ben_estadodesafio string,
cod_ben_tiempo string,
cod_ben_nivel string

)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/beneficios/dim/dim_ben_desafios'
