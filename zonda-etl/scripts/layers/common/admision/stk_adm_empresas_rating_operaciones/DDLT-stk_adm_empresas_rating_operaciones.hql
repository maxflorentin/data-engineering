CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_rating_operaciones (
    id_adm_vo bigint,
    cod_adm_nro_prop bigint,
    cod_adm_secuencia int,
    cod_adm_operacion string,
    dec_adm_plazo decimal(16, 2),
    flag_adm_unidad_plazo string,
    int_adm_atraso int,
    dec_adm_punt_plazo decimal(16, 2),
    dec_adm_punt_antiguedad decimal(16, 2),
    dec_adm_valoracion_operacion decimal(16, 2),
    cod_adm_peusualt string,
    ts_adm_pefecalt string,
    cod_adm_peusumod string,
    ts_adm_pefecmod string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/admision/stk_adm_empresas_rating_operaciones';