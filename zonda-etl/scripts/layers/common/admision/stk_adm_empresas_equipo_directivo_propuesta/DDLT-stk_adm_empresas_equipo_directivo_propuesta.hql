CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_equipo_directivo_propuesta (
    cod_adm_nro_prop bigint,
    cod_adm_secuencia bigint,
    ds_adm_nombre string,
    ds_adm_cargo string,
    flag_adm_cliente int,
    cod_adm_peusualt string,
    cod_adm_peusumod string,
    ts_adm_pefecalt string,
    ts_adm_pefecmod string,
    cod_adm_penumper int,
    cod_per_nup int,
    cod_adm_cuit bigint,
    cod_adm_tipo_doc string,
    cod_adm_nro_doc bigint,
    int_adm_anio_nacimiento int,
    ds_adm_titulo_prof string,
    flag_adm_inmuebles_propios int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_equipo_directivo_propuesta';