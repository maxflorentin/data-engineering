CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_ate_cartera_ejecutivo (
cod_per_nup                                         BIGINT,
ds_ate_modelo_atencion                                   STRING,
ds_ate_oficina                                           STRING,
ds_ate_legajo                                            STRING,
ds_ate_nombre                                            STRING,
ds_ate_apellido                                          STRING,
ds_ate_puesto                                            STRING,
ds_ate_ejecutivo                                         STRING,
partition_date                                           STRING
)
PARTITIONED BY ( partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/modelo_atencion/fact/stk_ate_cartera_ejecutivo'