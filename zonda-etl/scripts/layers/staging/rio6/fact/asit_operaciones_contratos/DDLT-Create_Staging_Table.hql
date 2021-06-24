CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_operaciones_contratos(
    asoc_cd_operacion       string,
    asoc_cd_sub_operacion   string,
    asoc_fe_operacion       string,
    asoc_cd_usuario         string,
    asoc_cd_ramo            string,
    asoc_nu_contrato        string,
    asoc_nu_certificado     string,
    asoc_nu_endoso          string,
    asoc_de_endoso          string,
    asoc_tp_documento       string,
    asoc_nu_documento       string,
    asoc_de_operacion       string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/asit_operaciones_contratos';