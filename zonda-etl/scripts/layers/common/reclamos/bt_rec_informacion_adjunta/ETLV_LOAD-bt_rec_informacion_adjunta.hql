SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
INSERT
	OVERWRITE TABLE bi_corp_common.bt_rec_informacion_adjunta PARTITION(partition_date)
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(ide_gestion_sector) cod_rec_sector ,
	trim(ide_gestion_nro) cod_rec_gestion_nro ,
	trim(cod_actor) cod_rec_actor ,
	trim(nro_orden) int_rec_actor ,
	trim(indi_tpo_info) cod_rec_tipo_info ,
	IF(cod_info_doc_reque = 'null', NULL, TRIM(cod_info_doc_reque)) cod_rec_info_reque ,
	IF(dato_info_doc_reque = 'null'
	OR LENGTH(dato_info_doc_reque) = 0, NULL, TRIM(dato_info_doc_reque)) ds_rec_dato_info_reque ,
	IF(link_inf_adjunta = 'null', NULL, TRIM(link_inf_adjunta)) ds_rec_link_inf_adjunta ,
	trim(fec_inf_adjunta) dt_rec_info_adjunta ,
	trim(user_inf_adjunta) ds_rec_usuario_info_adjunta ,
	trim(cod_sect_inf_adjunta) cod_rec_sector_info_adjunta ,
	IF(nom_archivo_original = 'null', NULL, TRIM(nom_archivo_original)) ds_rec_nombre_archivo_original ,
	IF(nom_archivo = 'null', NULL, TRIM(nom_archivo)) ds_rec_nombre_archivo ,
	IF(nom_directorio = 'null', NULL, TRIM(nom_directorio)) ds_rec_nombre_directorio ,
	IF(tamano = 'null', NULL, TRIM(tamano)) int_rec_tamano_inf_adj ,
	trim(secuencia) int_rec_secuencia_inf_adj ,
	partition_date
FROM
	bi_corp_staging.rio56_informacion_adjunta
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'


