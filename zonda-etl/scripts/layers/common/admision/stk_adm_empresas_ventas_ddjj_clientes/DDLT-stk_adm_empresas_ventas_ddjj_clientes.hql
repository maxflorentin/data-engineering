CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_ventas_ddjj_clientes (
    id_adm_ddjj bigint,
    cod_adm_penumper bigint,
    id_adm_venta bigint,
    fc_adm_venta decimal(15,2),
    dt_adm_venta string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_ventas_ddjj_clientes';