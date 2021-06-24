CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_cuotas_contratos(
    ascc_cd_ramo             string,
    ascc_nu_contrato         string,
    ascc_nu_certificado      string,
    ascc_nu_endoso           string,
    ascc_nu_cuota            string,
    ascc_fe_desde            string,
    ascc_fe_hasta            string,
    ascc_mt_cuota            string,
    ascc_mt_comision         string,
    ascc_st_cuota            string,
    ascc_fe_status           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/asit_cuotas_contratos';