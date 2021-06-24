CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_clasif_crm (
    cod_entidad  string,
    tpo_pers  string,
    cod_crm  string,
    desc_crm  string,
    id_clasif_select  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/clasif_crm'