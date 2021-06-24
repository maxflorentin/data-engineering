CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_zen_ticket (
	ds_zen_url string,
	cod_zen_ticket bigint,
	cod_zen_external string,
	ds_zen_canal string,
	ts_zen_creacion timestamp,
	ts_zen_actualizacion timestamp,
	ds_zen_asunto string,
	ds_zen_ticket string,
	ds_zen_prioridad string,
	ds_zen_status string,
	ds_zen_destinatario string,
	cod_zen_usuario_solicitante bigint,
	ds_zen_usuario_solicitante string,
	cod_zen_usuario_remitente bigint,
	ds_zen_usuario_remitente string,
	cod_zen_usuario_asignado bigint,
	ds_zen_usuario_asignado string,
	cod_zen_organizacion string,
	cod_zen_grupo bigint,
	ds_zen_grupo string,
	cod_zen_usuarios_colaboradores string,
	cod_zen_email_cc string,
	flag_zen_publico int,
	ds_zen_etiquetas string,
	ds_zen_campos_personalizados string,
	ds_zen_satisfaccion string,
	cod_zen_form bigint ,
	ds_zen_form string,
	cod_zen_brand bigint,
	ds_zen_brand string
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/fact/bt_zen_ticket';