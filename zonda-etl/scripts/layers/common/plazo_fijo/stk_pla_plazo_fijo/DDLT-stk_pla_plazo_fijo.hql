CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_pla_plazofijo(
    cod_pla_entidad string,
    cod_pla_sucursal bigint,
    ds_pla_sucursal string,
    cod_pla_cuenta bigint,
    cod_pla_producto string,
    cod_pla_subproducto string,
    ds_pla_producto string,
    cod_pla_divisa string,
    cod_pla_secuencia int,
    cod_per_nup bigint,
    cod_pla_secrenovacion int,
    fc_pla_plazo int,
    cod_pla_estado string,
    ds_pla_estado string,
    fc_pla_saldoinicial decimal(15,2),
    fc_pla_tasa decimal(8,5),
    cod_pla_canalapertura string,
    ds_pla_canalapertura string,
    fc_pla_impcta decimal(12,2),
    fc_pla_impsincta decimal(12,2),
    fc_pla_inteabo decimal(15,2),
    fc_pla_intperiodo decimal(15,2),
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/plazo_fijo/fact/stk_pla_plazofijo';