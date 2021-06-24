CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_prodplanes(
    capb_carp_cd_ramo            string,
    capb_capu_cd_producto        string,
    capb_cd_plan                 string,
    capb_fe_inicio               string,
    capb_de_plan                 string,
    capb_mt_maximo               string,
    capb_cacm_cd_compania        string,
    capb_po_comision             string,
    capb_in_inspeccion           string,
    capb_mt_minimo               string,
    capb_nu_edad_min             string,
    capb_nu_edad_max             string,
    capb_st_plan                 string,
    capb_de_plan_larga           string,
    capb_nu_edad_anu             string,
    capb_cd_categoria            string,
    capb_cd_clasificacion        string,
    capb_po_coeficiente          string,
    capb_cd_producto_renovar     string,
    capb_cd_plan_renovar         string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_prodplanes';