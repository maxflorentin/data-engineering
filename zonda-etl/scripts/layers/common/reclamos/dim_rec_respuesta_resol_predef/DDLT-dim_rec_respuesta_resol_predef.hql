CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_respuesta_resol_predef (
	cod_rec_entidad string,
    cod_rec_rta_resol_predef int,
    ds_rec_rta_resol_predef string,
    ds_rec_detall_rta_resol string,
    flag_rec_tipo_respuesta int,
    cod_rec_est_rta_resol_predef string,
    cod_rec_usuario_alta_rta_resol_predef string,
    ts_rec_alta_rta_resol_predef timestamp,
    cod_rec_usuario_modif_rta_resol_predef string,
    ts_rec_modif_rta_resol_predef timestamp,
    cod_reg_tipo_resolucion int,
    flag_rec_opcion_acm int ,
    cod_rec_sector_owner string,
	partition_date string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_respuesta_resol_predef';