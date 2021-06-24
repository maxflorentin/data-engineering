CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_inhabilitados ( 
	cod_cys_inh	bigint ,
	cod_per_tipopersona	string ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_per_sexo	string ,
	dt_per_fechanacimiento	string ,
	ds_per_tipodoc	string ,
	ds_per_numdoc	bigint ,
	cod_cys_causal	string ,
	ds_cys_causal string ,
	int_cys_seccaus	int ,
	dt_cys_inh	string ,
	dt_cys_rehb	string ,
	flag_cys_vigencia	int ,
	int_cys_diasantiguedad int )
PARTITIONED BY ( partition_date string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_cys_inhabilitados'