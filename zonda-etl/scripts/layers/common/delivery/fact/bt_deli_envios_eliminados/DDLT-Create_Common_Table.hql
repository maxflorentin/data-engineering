CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_envios_eliminados (
               cod_deli_crupier int
              ,ts_deli_eliminado TIMESTAMP
              ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_envios_eliminados'
;