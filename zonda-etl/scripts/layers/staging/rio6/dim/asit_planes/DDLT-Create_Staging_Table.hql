CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_planes(
    aspl_cd_plan       string,
    aspl_fe_inicio     string,
    aspl_de_plan       string,
    aspl_po_comision   string,
    aspl_edad_min      string,
    aspl_edad_max      string,
    aspl_st_plan       string,
    aspl_fe_status     string,
    aspl_prioridad     string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_planes';