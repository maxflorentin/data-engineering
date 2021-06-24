CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_ifrs9ajustesadicionalesnup (
	dt_cys_periodo	string ,
	ds_cys_portfolioifrs9	string ,
	ds_cys_nombrecliente string ,
	cod_per_nup bigint ,
	ds_cys_segmentocontrol	string ,
	fc_cys_drawnbalance	decimal(38,15) ,
	fc_cys_undrawnbalance	decimal(38,15) ,
	fc_cys_ead	decimal(38,15) ,
	fc_cys_provisioninbalanceamount	decimal(38,15) ,
	fc_cys_provisionoutbalanceamount decimal(38,15) ,
	fc_cys_provision decimal(38,15) ,
	fc_cys_stagefinal int ,
	fc_cys_rof	decimal(38,15) ,
	fc_cys_insolvenciasrof	decimal(38,15) ,
	fc_cys_nuevorof	decimal(38,15) ,
	ds_cys_origen string ,
	cod_cys_rubrocargabalinprovision string ,
	cod_cys_rubrocargabaloutprovision string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_cys_ifrs9ajustesadicionalesnup'