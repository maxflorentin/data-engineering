CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_movimientos ( 
	cod_suc_canal string ,
	cod_suc_sector string ,
	cod_per_nup	int ,
	cod_suc_maquina	string ,
	dt_suc_fechatransaccion	string ,
	ts_suc_horatransaccion	string ,
	cod_per_tipopersona	string ,
	cod_suc_sucursalcuenta	int ,
	cod_suc_sucursalmaquina	int ,
	cod_suc_transaccion	string ,
	ds_suc_transaccion	string ,
	cod_suc_originalreverso	string ,
	cod_suc_usuario	string ,
	cod_per_cuadrante	string ,
	ds_per_subsegmento	string ,
	fc_suc_importetransaccion	decimal(18,2) ,
	cod_div_divisa	string ,
	fc_suc_cantidadefectivo	bigint ,
	fc_suc_importeefectivo	decimal(18,2) ,
	fc_suc_cantidadcheques	int ,
	fc_suc_importecheques	decimal(18,2) ,
	fc_suc_cantidadotros	bigint ,
	fc_suc_importeotros	decimal(18,2) ,
	cod_suc_mediopago	string ) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_movimientos'