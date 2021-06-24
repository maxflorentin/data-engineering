CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_posicion (
    id_posicion string,
    nombre string,
    id_empresa string,
    activo string,
    posicion_tipo_id string,
    posicion_sub_tipo_id string,
    numero_id string,
    numero_id_cont string,
    origen string,
    mail string,
    gavetas_rotas string,
    co string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/dim/ba_posicion';