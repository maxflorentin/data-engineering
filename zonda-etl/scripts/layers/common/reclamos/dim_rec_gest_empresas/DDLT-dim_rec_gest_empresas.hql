CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_gest_empresas (
  cod_rec_entidad string
  ,cod_rec_sector string
  ,cod_rec_gestion_nro int
  ,cod_rec_tipo_doc_empr string
  ,cod_rec_nro_doc_empr int
  ,cod_rec_sec_empr int
  ,cod_rec_tipo_empr string
  ,dt_rec_ini_activ_empr string
  ,cod_rec_activ_empr string
  ,ds_rec_nom_comer_empr string
  ,ds_rec_razon_social_empr  string
  ,cod_per_nup int
  ,ds_rec_apellido_resp_empr string
  ,ds_rec_nombre_resp_empr string
  ,cod_rec_tipo_doc2_empr string
  ,int_rec_tipo_doc2_empr int
  ,cod_suc_sucursal_empr int
  ,cod_rec_segmento_empr string
  ,cod_rec_crm_empr string
  ,ds_rec_crm_empr string
  ,cod_rec_clasif_select_empr int
  ,ds_rec_semf_ingreso_crm_empr string
  ,ds_rec_semf_renta_crm_empr string
  ,ds_rec_semf_riesgo_crm_empr string
  ,fc_rec_rentabilidad_promedio_empr decimal(14,2)
  ,ds_rec_color_semf_empr string
  ,ds_rec_color_semf_riesgo_empr string
  ,partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_gest_empresas';