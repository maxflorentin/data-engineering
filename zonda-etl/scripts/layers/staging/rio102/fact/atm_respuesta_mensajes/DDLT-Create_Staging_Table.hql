CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_atm_respuesta_mensajes(
    tarjeta string ,
    nup string ,
    mensaje string ,
    resp_cliente_novedad string ,
    resp_cliente_maestro string ,
    fecha_alta string ,
    fecha_baja string ,
    sigla_atm string ,
    fecha_oferta string ,
    hora_oferta string ,
    tipo_telefono string ,
    codigo_area string ,
    numero_tel string ,
    tip_doc string ,
    num_doc string ,
    as_of_date string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/atm_respuesta_mensajes';