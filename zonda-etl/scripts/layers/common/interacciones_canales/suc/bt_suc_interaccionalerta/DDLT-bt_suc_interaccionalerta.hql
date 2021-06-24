CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccionalerta ( 
	cod_suc_interaccionalerta string,
	cod_suc_interaccion string,
	cod_suc_alerta int,
	cod_suc_resultado int,
	cod_suc_gestion int,
	dt_suc_gestion string,
	ts_suc_gestion timestamp,
	ds_suc_claveunica string,
	ds_suc_json string,
	ds_suc_contacto string,
	cod_suc_identificacionresultado int) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccionalerta'