CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccionproducto ( 
	cod_suc_interaccionproducto string,
	cod_suc_interaccion string,
	cod_suc_acc int,
	ds_suc_campanaoproducto string,
	flag_suc_campana int,
	cod_suc_escenario int,
	cod_suc_envio int,
	ds_suc_productocrm string,
	cod_suc_gestion int,
	cod_suc_resultado int,
	ds_suc_json string,
	ds_suc_comentario string,
	flag_suc_enviomail int,
	dt_suc_agendahorario string,
	ts_suc_agendahorario timestamp,
	dt_suc_modificacion string,
	ts_suc_modificacion timestamp,
	dt_suc_gestion string,
	ts_suc_gestion timestamp,
	cod_suc_identificacionresultado int,
	cod_suc_productocrm string,
	cod_suc_vlnumerosolicitud int,
	cod_suc_canalsolicitud string,
	cod_suc_campbuc string, 
	ds_suc_contacto string,
	flag_suc_seguimientotec int	) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccionproducto'