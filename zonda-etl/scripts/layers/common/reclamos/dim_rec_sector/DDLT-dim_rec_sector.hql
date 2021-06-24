CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_sector ( 
	cod_rec_entidad string ,
	cod_rec_sector string ,
	ds_rec_sector string ,
	cod_rec_grupo_sector string ,
	cod_rec_actral_sector string ,
	cod_rec_pcia_sector int ,
	ds_rec_mail_sector string ,
	cod_rec_estado_sector string ,
	flag_rec_enviar_mail_sector int ,
	flag_rec_info_asig_esp_sector int ,
	cod_rec_pais_sector string ,
	flag_rec_resol_info_adj_sector int ,
	cod_rec_ctro_costos_sector string ,
	flag_rec_mail_bandeja_sector int ,
	flag_rec_uso_carta_sector int ,
	cod_rec_sector_owner_sector string ,
	cod_rec_grupo_empresa_sector int ,
	flag_rec_admin_sector int ,
	fc_rec_gest_x_pag_bandeja_sector int ,
	cod_suc_sucursal_sector int ,
	cod_rec_zona_sector string ,
	cod_rec_tipo_sector int ,
	ds_rec_class_pdf_sector string ,
	fc_rec_cant_gestiones_ant_sector int ,
	cod_rec_sector_gen string ,
	flag_rec_compromiso_gold_sector int ,
	flag_rec_enviar_mail_resp_sector int ,
	flag_rec_enviar_sms_resp_sector int ,
	partition_date string ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_sector';