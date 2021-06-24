CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_producto_demanda_mercado (
    cod_adm_nro_prop bigint,
    cod_adm_tpo_cliente string,
    cod_adm_gdo_cliente string,
    cod_adm_antig_cliente string,
    flag_adm_dentro_radio int,
    ds_adm_posicion string,
    flag_adm_visitado int,
    dt_adm_ultima_visita string,
    flag_adm_marco_visita int,
    ds_adm_marco_visita string,
    int_adm_can_empleados int,
    int_adm_can_clientes int,
    dec_adm_porc_ventas_credit decimal(3, 2),
    flag_adm_datos_grabados int,
    int_adm_antig_socios int,
    flag_adm_aval int,
    flag_adm_otros_negocios int,
    ds_adm_otros_negocios string,
    ds_adm_funcionamiento string,
    flag_adm_gcia_distinta int,
    cod_adm_usu_alta string,
    ts_adm_alta string,
    cod_adm_usu_mod string,
    ts_adm_mod string,
    ds_adm_producto1 string,
    ds_adm_producto2 string,
    ds_adm_producto3 string,
    dec_adm_porc_producto1 decimal(3, 2),
    dec_adm_porc_producto2 decimal(3, 2),
    dec_adm_porc_producto3 decimal(3, 2),
    ds_adm_datos_grabados string,
    flag_adm_deuda int,
    ds_adm_deuda string,
    flag_adm_verificado int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_producto_demanda_mercado';