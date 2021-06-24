CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.bt_personas_bajas

(
dt_per_fechainformacion                 string,
cod_per_nup                             int,
int_antiguedad_titular                  int,
dt_alta_titular                         string,
ds_mejor_produ                          string,
ds_mejor_produ_mes_ant                  string,
ds_mejor_produ_semestre                 string,
ds_mot_baja_mejor_prod_mes_ant          string,
ds_mot_baja_mejor_prod_semestre         string)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/personas/bt_personas_bajas'