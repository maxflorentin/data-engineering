CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_equipos (
    sigla string,
    nro_serie string,
    equipo string,
    suc string,
    f_alta string,
    modelo string,
    apertura string,
    operador string,
    reconoc_bill string,
    funcionalidad string,
    id_tipo_posicion string,
    id_posicion string,
    id_ubicacion string,
    id_sed string,
    id_equipos string,
    buzon_dep string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/dim/ba_equipos';