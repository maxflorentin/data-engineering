CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_clientes_prop (
cod_adm_nro_prop int,
ds_adm_nom_cliente string,
cod_adm_cliente int,
int_adm_plz_cobro int,
int_adm_porc_ventas int,
ds_adm_ref_obtenidas string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_clientes_prop';