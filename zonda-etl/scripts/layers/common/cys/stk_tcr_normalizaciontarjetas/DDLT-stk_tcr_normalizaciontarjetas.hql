CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_tcr_normalizaciontarjetas (
	cod_per_nup	bigint ,
	cod_tcr_entidad	string ,
	cod_suc_sucursal	int ,
	cod_nro_cuenta	bigint ,
	cod_prod_producto	string ,
	cod_prod_subproducto	string ,
	cod_div_divisa	string ,
	int_tcr_secuencia	int ,
	dt_tcr_fechacuotaphone	string ,
	cod_tcr_cuentabase	bigint ,
	cod_tcr_marcainicial	string ,
	cod_tcr_submarcainicial	string ,
	cod_tcr_marcasegini	string ,
	cod_tcr_tiporeestructuracionini	string ,
	cod_tcr_marcaactual	string ,
	cod_tcr_submarcaactual	string ,
	dt_tcr_fechacambioseg	string ,
	cod_tcr_marcasegact	string ,
	cod_tcr_tiporeestructuracionact	string ,
	dt_tcr_fechacura	string ,
	dt_tcr_fechaempeora	string ,
	dt_tcr_fechanormaliza	string ,
	ds_tcr_motivocambio	string ,
	int_tcr_ree	int ,
	dt_tcr_fechacarga	string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_tcr_normalizaciontarjetas' ;