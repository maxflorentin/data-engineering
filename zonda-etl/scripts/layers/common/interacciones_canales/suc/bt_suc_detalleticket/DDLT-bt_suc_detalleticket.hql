CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_detalleticket ( 
	dt_suc_atencion	string ,
	cod_suc_sucural	int ,
	cod_suc_zona	string ,
	ds_suc_tipocola	string ,
	cod_suc_ticket	string ,
	int_suc_servicio	int ,
	ds_suc_servicio	string ,
	int_suc_puesto	int ,
	ds_suc_puesto	string ,
	cod_suc_legajo	string ,
	ds_suc_nombreusuario	string ,
	ts_suc_objespera	string ,
	ts_suc_maxespera	string ,
	ts_suc_ingreso	string ,
	ts_suc_llamado	string ,
	ts_suc_finatencion	string ,
	ts_suc_espera	string ,
	ts_suc_tiempoatencion	string ,
	ds_suc_auxiliar1	string ,
	ds_suc_auxiliar2	string ,
	cod_suc_motivovisita	string ,
	ds_suc_motivovisita	string ,
	cod_suc_estado	string ,
	ds_suc_motivoabandono	string ,
	int_suc_tecn	bigint )
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_detalleticket'