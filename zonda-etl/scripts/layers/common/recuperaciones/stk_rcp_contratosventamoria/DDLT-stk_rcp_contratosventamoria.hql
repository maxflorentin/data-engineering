CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_contratosventamoria (
  	cod_rcp_venta int,
	cod_suc_sucursal int, --
	cod_nro_cuenta string,
	cod_pro_producto int,
	cod_pro_subproducto string,
	cod_div_divisa string,
	cod_per_nup bigint,
	cod_rcp_tipodocumento string,
	ds_rcp_documento bigint,
	ds_rcp_penomfan int,
	ds_rcp_apellidocliente int,
	ds_rcp_nombrecliente string,
	cod_rcp_tipodocumentoi string,
	ds_rcp_documentoi bigint,
	ds_rcp_apellidoclientei string,
	ds_rcp_nombreclientei string,
	fc_rcp_totalcapital decimal(17,4),
	fc_rcp_totalinteres decimal(17,4),
	fc_rcp_totalcomision decimal(17,4),
	fc_rcp_totalgastos decimal(17,4),
	fc_rcp_totalseguros decimal(17,4),
	fc_rcp_totalimpuesto decimal(17,4),
	fc_rcp_totalajuste decimal(17,4),
	fc_rcp_saldo decimal(17,4),
	fc_rcp_saldoinformadoi decimal(17,4),
	fc_rcp_importerecupera decimal(17,4),
	fc_rcp_imptotcanco decimal(17,4),
	fc_rcp_impcancgasc decimal(17,4),
	fc_rcp_totalcontable decimal(17,4),
	fc_rcp_totalnocontable decimal(17,4),
	fc_rcp_totalimpuperc decimal(17,4),
	fc_rcp_cantidadpagos int,
	cod_rcp_estado string,
	dt_rcp_fechacastigo string,
	cod_rcp_origencast string,
	dt_rcp_fechaabseguimiento string,
	dt_rcp_fechaabtotal string,
	dt_rcp_fechabaja string,
	cod_rcp_motivobaja string,
	dt_rcp_fechainisituirregular string,
	cod_rcp_coefinde string,
	cod_rcp_refinanciacion string,
	flag_rcp_conversi int,
	cod_rcp_tipadmin string,
	cod_rcp_procedencia string,
	ds_rcp_nombreeejj string,
	dt_rcp_fechaalta string,
	ds_rcp_usuarioalta string,
	ds_rc_usuarioumo string,
	ts_rcp_umo timestamp,
	cod_rcp_productoemerix string,
	cod_rcp_tipocartera string,
	cod_rcp_escenario string,
	cod_rcp_bufette int,
	ds_rcp_segmentocomercial string,
	flag_rcp_rechazo int,
	cod_rcp_rechazo string,
	ds_rcp_motivorechazo string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_contratosventamoria' ;