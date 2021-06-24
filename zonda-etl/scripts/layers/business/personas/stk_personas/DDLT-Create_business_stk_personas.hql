CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.stk_personas
(
dt_per_fechainformacion string,
cod_per_nup             int,
ds_mejor_relacion       string,
flag_titular            int,
flag_titular_gym        int,
int_antiguedad_titular  int,
dt_alta_titular         string,
ds_mejor_produ          string,
ds_mejor_produ_mes_ant  string,
ds_mejor_produ_semestre string)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/personas/stk_personas'
