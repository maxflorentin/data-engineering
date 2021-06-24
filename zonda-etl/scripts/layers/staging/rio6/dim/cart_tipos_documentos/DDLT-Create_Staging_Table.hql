CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_tipos_documentos(
    catd_descripcion    string,
    catd_abreviatura    string,
    catd_status         string,
    catd_rector         string,
    catd_banco          string,
    catd_mapfre         string,
    catd_meridional     string,
    catd_royal          string,
    catd_ace            string,
    catd_aon            string,
    catd_herzfeld       string,
    catd_lacaja         string,
    catd_assurant       string,
    catd_zurich         string,
    catd_sancor         string,
    catd_qbe            string,
    catd_orbis          string,
    catd_triunfo        string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_tipos_documentos';