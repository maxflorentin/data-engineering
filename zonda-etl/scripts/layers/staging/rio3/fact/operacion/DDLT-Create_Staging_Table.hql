CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_operacion(
    operacion string,
    contacto string,
    origen string,
    medio string,
    llamada string,
    usuario string,
    fecha string,
    hora string,
    gestion string,
    duracion string,
    dia_rel string,
    hor_rel string,
    com_rel string,
    unico_ope string,
    vfecha string,
    vdesde string,
    vhasta string,
    vcomenta string,
    exportada string,
    vgestor string,
    canal string,
    canal_comunicacion string,
    sucursal string,
    llamada2 string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/operacion';