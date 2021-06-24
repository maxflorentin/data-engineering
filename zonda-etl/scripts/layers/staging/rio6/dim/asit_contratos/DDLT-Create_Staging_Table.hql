CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_contratos(
    asct_cd_ramo                   string,
    asct_nu_contrato               string,
    asct_nu_certificado            string,
    asct_nu_endoso                 string,
    asct_tp_documento              string,
    asct_nu_documento              string,
    asct_fe_contratacion           string,
    asct_mt_cuota                  string,
    asct_mt_comision               string,
    asct_st_certificado            string,
    asct_fe_ultimo_cambio          string,
    asct_fe_desde                  string,
    asct_fe_hasta                  string,
    asct_cd_provincia              string,
    asct_cd_ciudad                 string,
    asct_calle                     string,
    asct_numero                    string,
    asct_piso                      string,
    asct_depto                     string,
    asct_cd_postal                 string,
    asct_cpa                       string,
    asct_fe_anulacion              string,
    asct_cd_causa_anulacion        string,
    asct_fe_rehabilitacion         string,
    asct_cd_producto               string,
    asct_cd_plan                   string,
    asct_cd_sucursal               string,
    asct_nu_cotizacion             string,
    asct_de_observacion            string,
    asct_nu_contrato_cedente       string,
    asct_nu_cert_cedente           string,
    asct_tp_doc_tomador            string,
    asct_nu_doc_tomador            string,
    asct_cd_moneda                 string,
    asct_nu_contrato_original      string,
    asct_tp_cuenta                 string,
    asct_nu_cuenta                 string,
    asct_cd_canal                  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_contratos';