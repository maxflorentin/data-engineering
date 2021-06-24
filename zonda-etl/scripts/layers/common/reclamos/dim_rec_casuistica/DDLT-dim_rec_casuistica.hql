CREATE EXTERNAL TABLE bi_corp_common.dim_rec_casuistica (
  cod_rec_casuistica INT,
  ds_rec_casuistica STRING,
  cod_rec_tipo_gestion INT,
  cod_rec_tipologia STRING,
  fc_rec_cant_meses_primer_reclamo INT,
  fc_rec_cant_dias_resolucion INT,
  cod_rec_tipo_producto STRING,
  cod_rec_sector_soporte STRING,
  flag_rec_habilitado INT,
  cod_rec_usuario_alta STRING,
  dt_rec_casuistica_alta TIMESTAMP,
  cod_rec_usuario_modif STRING,
  dt_rec_casuistica_modif TIMESTAMP,
  ds_rec_texto_mail_casuistica STRING,
  flag_rec_interes_comp INT,
  dt_rec_casuistica_baja TIMESTAMP,
  flag_rec_solo_activas INT,
  fc_rec_dias_warning_bcra INT,
  cod_rec_state STRING,
  ds_rec_sinonimos STRING,
  ds_rec_nombre_atajos STRING,
  ds_rec_desc_atajos STRING,
  cod_rec_secciones STRING,
  fc_rec_monto_desde_sucu DECIMAL(15,2),
  fc_rec_monto_desde_zona DECIMAL(15,2),
  fc_rec_cant_tope_mes INT,
  partition_date STRING
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_casuistica';