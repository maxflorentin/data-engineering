CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_ror_input_activo (	
	cod_ren_unidad string,
	dt_ren_data string,
	cod_ren_finalidaddatos string,
	cod_ren_contrato_rorac string,
	cod_ren_contrato string,
	cod_ren_areanegocio string,
	cod_ren_divisa string,
	cod_ren_division string,
	cod_ren_producto string,
	cod_ren_pers_ods string,
	fc_ren_margenmes DECIMAL(20,4),
	fc_ren_sdbsmv DECIMAL(20,4),
	fc_ren_sfbsmv DECIMAL(20,4),
	dt_ren_fec_altacto string,
	dt_ren_fec_ven string,
	cod_ren_entidad_espana string,
	cod_ren_centrocont string,
	cod_ren_subprodu string,
	cod_ren_zona string,
	cod_ren_territorial string,
	cod_ren_gestorprod string,
	cod_ren_gestor string,
	id_cto_bdr string,
	fc_ren_sdbsfm DECIMAL(20,4),
	fc_ren_interes DECIMAL(20,4),
	fc_ren_comfin DECIMAL(20,4),
	fc_ren_comnofin DECIMAL(20,4),
	cod_ren_vincula string,
	fc_ren_rof DECIMAL(20,4),
	fc_ren_tti DECIMAL(20,4),
	cod_ren_mtm string,
	cod_ren_nocional string,
	cod_ren_divisa_mis string,
	flag_ren_moralocal int,
	flag_ren_carterizadas int,
	ds_ren_nombrecliente string,
	cod_per_tipopersona string,
	cod_ren_nifcif string,
	ds_ren_intragrupo string,
	cod_ren_internegocio string,
	cod_ren_areanegociocorp string,
	cod_productogest string,
	cod_segmentogest string,
	cod_producto_operacional string,
	ds_ren_ficheromis string,
	dt_ren_fec_reestruc string,
	dt_ren_fec_extrdatos string,
	flag_ren_neteo int,
	fc_ren_orex DECIMAL(20,4),
	fc_ifrs_provision DECIMAL(20,4),
	fc_ren_costemensualcto DECIMAL(20,4),
    cod_nivel_operacion int
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/fact/bt_ror_input_activo'