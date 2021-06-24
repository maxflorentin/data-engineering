

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_ben_clientespremify (

cod_per_nup int ,
ds_ben_apellido string,
ds_ben_nombre string,
cod_ben_nivel string,
fc_ben_puntos int,
cod_ben_documento string,
cod_ben_tipodocumento string,
dt_ben_nacimientocliente string,
cod_ben_audiencia string,
flag_ben_aceptaterminos string,
dt_ben_desde string,
cod_ben_tag string,
ds_ben_tag string,
ds_ben_tagdescripcion string,
cod_ben_grupo string,
flag_ben_exlusivo string,
ds_ben_slug string

)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/beneficios/dim/stk_ben_clientespremify'
