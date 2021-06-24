CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_pla_plazofijo_rio35(
    cod_pla_plazofijo string,
    cod_pla_entidad string,
    cod_pla_sucursal string,
    ds_pla_sucursal string,
    cod_pla_cuenta string,
    cod_pla_producto string,
    cod_pla_subproducto string,
    ds_pla_producto string,
    cod_pla_divisa string,
    cod_pla_secuencia string,
    cod_per_nup string,
    cod_pla_secrenovacion string,
    fc_pla_plazo string,
    cod_pla_estado string,
    ds_pla_estado string,
    fc_pla_saldoinicial string,
    fc_pla_tasa string,
    cod_pla_canalapertura string,
    ds_pla_canalapertura string,
    fc_pla_impcta string,
    fc_pla_impsincta string,
    fc_pla_inteabo string,
    fc_pla_intperiodo string,
    dt_pla_fechaalta string,
    dt_pla_fechaopera string,
    dt_pla_fechavencimiento string,
    cod_pla_tarifavig string,
    cod_pla_indcustodia string)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/plazo_fijo/fact/stk_pla_plazofijo_rio35';