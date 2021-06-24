
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ben_beneficios (

  cod_ben_beneficio string,
  ds_ben_beneficio string,
  ds_ben_descripcion string,
  dt_ben_desde string,
  dt_ben_hasta string,
  ds_ben_usuarioautorizacion string,
  ds_ben_usuariomodificacion string,
  cod_ben_renovacionaut string,
  cod_ben_acreditacionretro string,
  dt_ben_desdeoriginal string,
  dt_ben_hastaoriginal string,
  cod_ben_inversion string,
  cod_ben_premio string,
  cod_ben_estado string,
  dt_ben_pausa string,
  ds_ben_terminosycondiciones string,
  cod_ben_categoriabeneficio string,
  cod_ben_operacion string,
  ds_ben_observaciones string,
  ds_ben_detalle string,
  cod_ben_nivel string,
  fc_ben_porcentajeacreditacion int,
  fc_ben_montotope int,
  fc_ben_montofijo int,
  fc_ben_condicionmonto int,
  fc_ben_transacciontope int,
  cod_ben_reembolso string,
  cod_ben_metodopago string,
  cod_ben_reembolsocodigooperacion string,
  ds_ben_metodopago string,
  cod_ben_prisma string,
  cod_ben_tipometodopago string
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/beneficios/dim/dim_ben_beneficios'
