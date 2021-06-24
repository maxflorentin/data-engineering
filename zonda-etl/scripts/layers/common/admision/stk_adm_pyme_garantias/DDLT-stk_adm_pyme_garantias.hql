CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_pyme_garantias (
cod_adm_nro_prop int,
cod_adm_gartia string,
ds_adm_detalle string,
fc_adm_mon_gartia double,
ds_adm_moneda_gartia string,
int_adm_secuencia int,
dec_adm_porc_gtia double,
cod_adm_seq_gartia int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_pyme_garantias';