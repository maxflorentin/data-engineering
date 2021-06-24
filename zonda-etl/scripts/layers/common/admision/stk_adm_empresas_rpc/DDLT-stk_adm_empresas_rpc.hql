CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_rpc (
    id_adm_rpc bigint,
    cod_adm_nro_prop bigint,
    fc_adm_imp_importe_calif decimal(16, 2),
    fc_adm_imp_resp_pat_comp decimal(16, 2),
    ds_adm_justificacion string,
    dt_adm_calificacion string,
    cod_adm_usu_alta string,
    dt_adm_estados_contables string,
    int_adm_duracion int,
    int_adm_rel_porc int,
    cod_adm_penumper int,
    flag_adm_imprimir_acta string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_rpc';