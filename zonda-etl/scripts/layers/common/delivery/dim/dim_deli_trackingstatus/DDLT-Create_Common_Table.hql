CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_trackingstatus (
                 cod_deli_estado int
                ,ds_deli_estado string
                ,ds_deli_motivo string
                ,ds_deli_motivo_secundario string
                ,cod_deli_estado_courier string
                ,ds_deli_courier string
                ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_trackingstatus';