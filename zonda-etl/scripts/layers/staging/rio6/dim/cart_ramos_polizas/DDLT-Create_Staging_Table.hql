CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_ramos_polizas(
    carp_cd_ramo                         string,
    carp_de_ramo                         string,
    carp_tp_ramo                         string,
    carp_cd_sigla                        string,
    carp_in_financiado                   string,
    carp_mt_maximo_pago                  string,
    carp_nu_meses_rehabilitar            string,
    capr_in_violacion                    string,
    carp_tp_grupo                        string,
    carp_in_particular_colectivo         string,
    carp_in_individual_colectivos        string,
    carp_in_anulacion                    string,
    carp_fe_ultima_renovacion            string,
    carp_st_renovacion                   string,
    carp_fe_calculo_renovacion           string,
    carp_in_solicitud_anulacion          string,
    carp_in_renovacion_automatica        string,
    carp_in_manejo_raco                  string,
    carp_in_coaseguro                    string,
    carp_nu_polizas_cliente              string,
    carp_mt_sumaseg_total                string,
    carp_de_web_select                   string,
    carp_st_ramo                         string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_ramos_polizas';