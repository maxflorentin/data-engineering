CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.raw_clientes_productos(
    nup double,
    nro_cuenta double,
    sucursal double ,
    tipo_participante string,
    orden_participante double,
    codigo_producto string,
    codigo_subproducto string,
    fecha_cierre string,
    estado string,
    marca_paquete string,
    motivo_baja string,
    fecha_alta string,
    nro_cuenta_ double)
  PARTITIONED BY (
    fecha_informacion string)
  ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/clientes_productos'