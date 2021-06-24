CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_ventas_post_balance_client (
    id_adm_ventas_post_balance_client int,
    int_adm_nro_prop int,
    ds_adm_penumper string,
    fc_adm_monto decimal(16,2),
    dt_adm_mes string,
    ds_adm_peusualt string,
    dt_adm_pefecalt string,
    ds_adm_peusumod string,
    dec_adm_unidades_fisicas_ext decimal(16,2),
    dec_adm_unidades_fisicas_int decimal(16,2),
    fc_adm_monto_dolares decimal(16,2),
    dt_adm_pefecmod string,
    dec_adm_cotizacion_dolar decimal(5,3),
    int_adm_nro_ultima_prop int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_ventas_post_balance_client';
