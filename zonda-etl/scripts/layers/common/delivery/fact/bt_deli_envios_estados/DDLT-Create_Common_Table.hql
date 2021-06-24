CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_envios_estados (
                cod_deli_estado_envio int
                ,cod_deli_estado int
                ,ts_deli_estado timestamp
                ,dt_deli_ultima_modificacion_estado string
                ,dt_deli_estado_solr string
                ,ds_deli_creador_estado string
                ,ds_deli_ultimo_modificador_estado string
                ,cod_deli_crupier int
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_envios_estados'