CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_cotiza_coberturas(
    cack_capj_cd_sucursal        string,
    cack_cazb_nu_cotizacion      string,
    cack_carp_cd_ramo            string,
    cack_capu_cd_producto        string,
    cack_capb_cd_plan            string,
    cack_carb_cd_ramo            string,
    cack_cacb_cd_cobertura       string,
    cack_nu_asegurado            string,
    cack_mt_comision             string,
    cack_mt_prima                string,
    cack_mt_suma_asegurada       string,
    cack_po_deducible            string,
    cack_mt_deducible            string,
    cack_po_recargo              string,
    cack_po_riesgo               string,
    cack_ta_tasa                 string,
    cack_mt_valor_real           string,
    cack_po_bonif                string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_cotiza_coberturas';