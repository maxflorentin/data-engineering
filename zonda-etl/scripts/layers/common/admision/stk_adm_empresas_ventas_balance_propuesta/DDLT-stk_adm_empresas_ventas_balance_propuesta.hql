CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_ventas_balance_propuesta (
    id_adm_venta bigint,
    id_adm_balance bigint,
    cod_adm_nro_prop bigint,
    fc_adm_mon_venta double,
    dt_adm_fec_venta string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_ventas_balance_propuesta';