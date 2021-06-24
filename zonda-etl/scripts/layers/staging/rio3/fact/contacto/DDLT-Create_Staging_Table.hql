CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_contacto(
    contacto string,
    tipodoc string,
    nrodoc string,
    expedida string,
    nacio string,
    fechaalta string,
    apellido string,
    nombre string,
    teledisc1 string,
    tel_1 string,
    postal1 string,
    teledisc2 string,
    tel_2 string,
    postal2 string,
    interno string,
    origen string,
    medio string,
    estado string,
    sexo string,
    usuario string,
    clifondos string,
    nivclifon string,
    cliente string,
    sucursal string,
    clavehost string,
    conytipodoc string,
    conynrodoc string,
    capturado string,
    incontactable string,
    cantoperac string,
    ultoperacion string,
    tipo_persona string,
    tipo_cliente string,
    estado2 string,
    fecha_modificacion string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/contacto';