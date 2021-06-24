CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_suc_sucursales (
	cod_suc_sucursal	int,
	ds_suc_sucursal	string,
	cod_suc_zona	string,
	ds_suc_zona	string,
	ds_suc_domiciliocalle	string,
	ds_suc_domicilionumero	string,
	ds_suc_localidad	string,
	ds_suc_provincia	string,
	cod_suc_codpostal	string,
	ds_suc_latitud	string,
	ds_suc_longitud	string,
	ds_suc_telefono	string,
	ds_suc_horario	string,
	ds_suc_gerente	string,
	ds_suc_estado	string,
	dt_suc_fechaalta	string,
	dt_suc_fechamodif	string,
	dt_suc_fechabaja	string,
	ds_suc_servicios	string
)
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/stk_suc_sucursales';
