CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_riesgos_cubiertos(
    carc_casu_cd_sucursal         string,
carc_carp_cd_ramo             string,
carc_capo_nu_poliza           string,
carc_cace_nu_certificado      string,
carc_fe_efectiva              string,
carc_nu_asegurado             string,
carc_carb_cd_ramo             string,
carc_cacb_cd_cobertura        string,
carc_mt_suma_asegurada        string,
carc_mt_prima_anual           string,
carc_mt_prima_bruta           string,
carc_mt_comision              string,
carc_ta_riesgo                string,
carc_po_comision              string,
carc_fe_inclusion             string,
carc_in_sumarizacion          string,
carc_capl_cd_plan             string,
carc_mt_valor_real            string,
carc_po_deducible             string,
carc_mt_deducible             string,
carc_calo_localidad           string,
carc_po_riesgo                string,
carc_po_descuento             string,
carc_po_recargo               string,
carc_mt_prima_aporte_cia      string,
carc_fe_desde                 string,
carc_fe_hasta                 string,
carc_in_aumento_fijo_prov     string,
carc_cd_bien                  string,
carc_po_participacion_bien    string,
carc_fe_mod_cobertura         string,
carc_mt_bono_produccion       string,
carc_po_bonif                 string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_riesgos_cubiertos';