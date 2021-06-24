CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_solicitud(
    solicitud string,
    circular string,
    promocion string,
    delivery string,
    canal string,
    sucursal string,
    familia string,
    programa string,
    lista string,
    vehiculo string,
    oferta1 string,
    oferta2 string,
    oferta3 string,
    anio string,
    mes string,
    estado string,
    cambioestado string,
    uniqueid string,
    solicitud_asol string,
    returncode_asol string,
    observaciones_asol string,
    nrocuenta_asol string,
    usuario string,
    fechaimpresol string,
    domicilioresumen string,
    promocion_asol string,
    ultoperacionvisita string,
    comentario string,
    campania string,
    prioperacion string,
    fecha_prioperacion string,
    circuito string,
    rpt_fcambio string,
    idjournal string,
    oficial string,
    solicitud_portal string,
    dependencia string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/solicitud';