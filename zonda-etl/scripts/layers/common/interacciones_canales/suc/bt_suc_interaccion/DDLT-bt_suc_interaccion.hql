CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccion ( 
	cod_suc_interaccion string,
	cod_per_nup bigint,
	cod_suc_legajo string,
	dt_suc_inicio string,
	ts_suc_inicio timestamp,
	dt_suc_cierre string,
	ts_suc_cierre timestamp,
	cod_suc_canalcomunicacion string,
	ds_suc_canalcomunicacion string,
	cod_suc_canalventa string,
	ds_suc_canalventa string,
	cod_suc_sucursal int,
	ds_suc_comentario string ) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccion'