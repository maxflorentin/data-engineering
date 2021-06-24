CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_zonal (
    id_zona string,
    descri_zonal string,
    area string,
    zona_nro string,
    zona_nombre string,
    id_usuario string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/dim/ba_zonal';