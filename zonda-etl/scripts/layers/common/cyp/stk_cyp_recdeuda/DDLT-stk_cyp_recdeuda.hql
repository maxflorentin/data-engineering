CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cyp_recdeuda(
cod_cyp_nro_empr					bigint,
cod_cyp_nro_digempr					int,
cod_cyp_nro_prod					int,
ds_cyp_nro_instan					int,
cod_cyp_nro_clte					string,
ds_cyp_tpo_cprb						string,
ds_cyp_nro_cprb						string,
ds_cyp_nro_cuo						string,
ts_cyp_alta_deud					string,
ds_cyp_nom_clte						string,
ds_cyp_direccion					string,
ds_cyp_localidad					string,
ds_cyp_codigopostal					string,
ds_cyp_nro_codigopostal				int,
ds_cyp_cod_ubiccpost				string,
ds_cyp_cuit_clte					bigint,
cod_cyp_ing_brutos					string,
cod_cyp_cond_iva					string,
dt_cyp_1er_vto						string,
fc_cyp_imp_1ervto					decimal(15,2),
dt_cyp_2do_vto						string,
fc_cyp_imp_2dovto					decimal(15,2),
dt_cyp_hasta_dscto					string,
fc_cyp_imp_prontopago				decimal(15,2),
dt_cyp_hasta_punit					string,
ds_cyp_tsa_puni						decimal(6,2),
ds_cyp_mar_excep3ra					string,
cod_cyp_form_pgo					string,
fc_cyp_imp_descto					decimal(15,2),
fc_cyp_imp_intdeven					decimal(15,2),
fc_cyp_mon_sdoact					decimal(15,2),
dt_cyp_ult_pgo						string,
fc_cyp_tot_sdoact					decimal(15,2),
cod_cyp_concep						string,
ds_cyp_doc							string,
ds_cyp_obs1							string,
ds_cyp_obs2							string,
ds_cyp_obs3							string,
ds_cyp_obs4							string,
ds_cyp_obs5							string,
ds_cyp_nro_proxrend					int,
cod_cyp_tpo_ingreso					string,
dt_cyp_alta							string,
ds_cyp_nro_envio					int,
cod_cyp_moneda						int,
cod_cyp_canal_fpago					string
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/stk_cyp_recdeuda'