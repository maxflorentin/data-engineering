CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_productos(
    aspr_cd_producto  string,
    aspr_de_producto  string,
    aspr_fe_inicio    string,
    aspr_cd_moneda    string,
    aspr_in_empleado  string,
    aspr_st_producto  string,
    aspr_fe_status    string,
    aspr_nu_cuotas    string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_productos';