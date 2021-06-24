CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_clasificacion_deudores_cliente (
    id_adm_cladeu bigint,
    cod_adm_penumper bigint,
    cod_adm_nro_cta_cte bigint,
    cod_adm_clasif_vigente string,
    cod_adm_clasif_definitiva string,
    ds_adm_com_plan_accion string,
    ds_adm_com_comite_banca string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_clasificacion_deudores_cliente';