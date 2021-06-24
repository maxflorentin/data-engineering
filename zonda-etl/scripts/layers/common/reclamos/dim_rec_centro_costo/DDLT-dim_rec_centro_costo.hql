CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_centro_costo (
    cod_suc_sucursal STRING ,          
	cod_rec_oficina STRING ,          
	ds_rec_centro_costo STRING ,         
	cod_rec_actor STRING ,         
	cod_rec_usuario_alta STRING ,          
	dt_rec_alta TIMESTAMP ,          
	cod_rec_usuario_modif STRING ,          
	dt_rec_modif TIMESTAMP ,          
	dt_rec_baja TIMESTAMP ,          
	cod_rec_zona INT ,          
	cod_rec_estado STRING ,          
	cod_rec_red STRING ,          
	ds_rec_domicilio STRING ,          
	ds_rec_localidad STRING ,          
	ds_rec_provincia STRING ,          
	cod_rec_cod_postal STRING ,          
	cod_rec_nro_cuenta_interna STRING ,         
	cod_per_nup INT ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_centro_costo';