CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_estadosoperacion (
    cod_adm_penumper int,
    cod_adm_secu_f487 int,
    dt_adm_fecha string,
    cod_adm_estado string,
    cod_adm_usuario string,
    id_adm_dev int,
    ds_adm_observaciones string,
    cod_adm_cargo_autorizante string,
    cod_adm_dentro_limite string,
    flag_adm_dentro_limite string,
    flag_adm_devolucion string,
    ds_adm_estado string,
    cod_adm_producto string,
    cod_adm_nro_prop int,
    cod_adm_operacion string,
    flag_adm_operacionespecial string,
    int_adm_cuit_cliente bigint,
    cod_adm_origen string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_estadosoperacion';