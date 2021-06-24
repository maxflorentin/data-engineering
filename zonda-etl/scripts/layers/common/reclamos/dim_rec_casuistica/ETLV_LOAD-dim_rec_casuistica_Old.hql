"SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;


DROP TABLE IF EXISTS max_casuistica;
CREATE TEMPORARY TABLE max_casuistica AS
SELECT trim(id_casuistica) id_casuistica,
	   max(partition_date) AS partition_date
FROM bi_corp_staging.cosmos_casuisticas
GROUP BY trim(id_casuistica)
;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_casuistica
SELECT
	TRIM(ori.id_casuistica) cod_rec_casuistica ,
	TRIM(UPPER(ori.desc_casuistica))  ds_rec_casuistica ,
	TRIM(ori.id_tipo_gestion) cod_res_tipo_gestion ,
	TRIM(ori.id_tipologia) cod_res_tipologia ,
	COALESCE(TRIM(ori.cant_meses_primer_reclamo),0) fc_rec_cant_meses_primer_reclamo ,
	COALESCE(TRIM(ori.cant_dias_resolucion),0) fc_rec_cant_dias_resolucion ,
	TRIM(ori.tipo_producto) cod_rec_tipo_producto ,
	TRIM(ori.id_sector_soporte) cod_rec_sector_soporte ,
	TRIM(ori.habilitado) flag_rec_habilitado ,
	TRIM(ori.id_usuario_alta) cod_rec_usuario_alta ,
	from_unixtime(unix_timestamp(ori.fecha_alta,'dd/MM/yyyy HH:mm:ss')) dt_rec_casuistica_alta ,
	TRIM(ori.id_usuario_modif) cod_rec_usuario_modif ,
	from_unixtime(unix_timestamp(ori.fecha_modif,'dd/MM/yyyy HH:mm:ss')) dt_rec_casuistica_modif ,
	TRIM(ori.texto_mail_casuistica) ds_rec_texto_mail_casuistica ,
	TRIM(ori.ind_interes_comp) flag_rec_interes_comp ,
	from_unixtime(unix_timestamp(ori.fecha_baja,'dd/MM/yyyy HH:mm:ss')) dt_rec_casuistica_baja ,
	TRIM(ori.ind_solo_activas) flag_rec_solo_activas ,
	COALESCE(TRIM(ori.dias_warning_bcra),0) fc_rec_dias_warning_bcra ,
	TRIM(ori.id_state) cod_rec_state ,
	TRIM(ori.sinonimos) ds_rec_sinonimos ,
	TRIM(ori.nombre_atajos) ds_rec_nombre_atajos ,
	TRIM(ori.desc_atajos) ds_rec_desc_atajos ,
	TRIM(ori.secciones) cod_rec_secciones ,
	TRIM(ori.monto_desde_sucu) fc_rec_monto_desde_sucu ,
	TRIM(ori.monto_desde_zona) fc_rec_monto_desde_zona ,
	TRIM(ori.cant_tope_mes) fc_rec_cant_tope_mes ,
	ori.partition_date partition_date
FROM
	bi_corp_staging.cosmos_casuisticas ori
INNER JOIN max_casuistica maxi
ON trim(ori.id_casuistica) =maxi.id_casuistica
AND ori.partition_date =maxi.partition_date;"