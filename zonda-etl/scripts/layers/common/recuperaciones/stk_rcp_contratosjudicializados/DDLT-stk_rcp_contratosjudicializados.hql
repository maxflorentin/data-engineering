CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_contratosjudicializados (
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
cod_per_nup bigint,
fc_rcp_importereclamado decimal(17,4),
cod_rcp_buffete string,
flag_rcp_indicasuspensionproceso int,
dt_rcp_fechasuspensionproceso string,
dt_rcp_fechaaltaprocedimiento string,
dt_rcp_fechabajaprocedimiento string,
ds_rcp_numeroposicion bigint,
ds_rcp_numeroordenjudicial bigint,
cod_rcp_ordenjudicial string,
ds_rcp_nroanioauto int,
ds_rcp_numjuzgado bigint,
cod_rcp_intervencion string,
ds_rcp_nroordeninterno string,
dt_rcp_fechabajacontrato string,
dt_rcp_fechaaltacontrato string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_contratosjudicializados'
;