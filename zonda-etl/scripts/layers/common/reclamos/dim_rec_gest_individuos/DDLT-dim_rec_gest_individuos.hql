CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_gest_individuos (
  cod_rec_entidad string
  , cod_rec_sector string
  , cod_rec_gestion_nro int
  , cod_rec_tipo_doc_indi string
  , cod_rec_nro_doc_indi int
  , dt_rec_nacimiento_indi string
  , cod_rec_sexo_indi string
  , ds_rec_apellido_indi string
  , ds_rec_nombre_indi string
  , cod_per_nup int
  , cod_suc_sucursal_indi int
  , cod_rec_segmento_indi string
  , cod_rec_crm_indi string
  , ds_rec_crm_indi string
  , cod_rec_clasif_select_indi int
  , ds_rec_semf_ingreso_crm_indi string
  , ds_rec_semf_renta_crm_indi string
  , ds_rec_semf_riesgo_crm_ind string
  , fc_rec_rentabilidad_promedio_indi decimal(14,2)
  , ds_rec_color_semf_indi string
  , ds_rec_color_semf_riesgo_indi string
  , partition_date string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_gest_individuos';