CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rec_gest_resol_acciones (
  cod_rec_gestion	INT ,
  cod_rec_accion	INT ,
  cod_rec_origen	STRING ,
  cod_rec_ejecucion	STRING )
PARTITIONED BY (
    partition_date STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/stk_rec_gest_resol_acciones';