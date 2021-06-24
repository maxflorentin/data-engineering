CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_posicion_domicilio (
    id_domicilio string,
    posicion_id string,
    provincia_id string,
    localidad_id string,
    calle string,
    numero string,
    piso string,
    depto string,
    postal string,
    postal_cpa string,
    f_alta string,
    f_baja string,
    u_alta string,
    u_modif string,
    tel string,
    f_mod string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/dim/ba_posicion_domicilio';