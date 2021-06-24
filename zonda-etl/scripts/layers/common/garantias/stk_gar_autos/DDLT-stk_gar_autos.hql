CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_gar_autos(

cod_gar_marca                       STRING,
cod_gar_modelo                      STRING,
cod_gar_anio                        STRING,
ds_gar_marca                        STRING,
ds_gar_modelo                       STRING,
ds_gar_tipo_vehiculo                STRING,
flag_gar_importado                  INT,
fc_gar_valor                        DECIMAL(19,4)

)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/garantias/fact/stk_gar_autos'