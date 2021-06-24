CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.garra_stk_log_cuotaphone (
gitccuo_cod_entigen STRING,
gitccuo_num_persona STRING,
gitccuo_cod_entidad STRING,
gitccuo_cod_centro STRING,
gitccuo_num_contrato STRING,
gitccuo_cod_producto STRING,
gitccuo_cod_subprodu STRING,
gitccuo_cod_divisa STRING,
gitccuo_num_secuencia INT,
gitccuo_fec_cuotaphone STRING,
gitccuo_cuenta_visa BIGINT,
gitccuo_cod_marca_ini STRING,
gitccuo_cod_submarca_ini STRING,
gitccuo_cod_marca_seg_ini STRING,
gitccuo_tip_reestruct_ini STRING,
gitccuo_cod_marca_act STRING,
gitccuo_cod_submarca_act STRING,
gitccuo_fec_cambio_seg STRING,
gitccuo_cod_marca_seg_act STRING,
gitccuo_tip_reestruct_act STRING,
gitccuo_fec_cura STRING,
gitccuo_fec_empeora STRING,
gitccuo_fec_normaliza STRING,
gitccuo_cod_criterio STRING,
gitccuo_motivo_cambio STRING,
gitccuo_num_ree INT,
gitccuo_entidad_umo STRING,
gitccuo_centro_umo STRING,
gitccuo_userid_umo STRING,
gitccuo_netname_umo STRING,
gitccuo_timest_umo STRING
)
PARTITIONED BY (
   partition_date STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garra/fact/stk_log_cuotaphone';
