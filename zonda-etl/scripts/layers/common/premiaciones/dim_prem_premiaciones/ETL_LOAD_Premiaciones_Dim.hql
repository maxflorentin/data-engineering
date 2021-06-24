
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TEMPORARY TABLE bi_corp_common.rel_sol_eve AS

	SELECT
id_solicitud,
id_evento
from
(
Select
	 	row_number() OVER (PARTITION BY
	 									id_solicitud
                             ORDER BY  fecha_carga desc) AS orden,
		id_solicitud,
		id_evento
	FROM bi_corp_staging.rio102_prem_solicitudes_eventos
	) a
	Where  a.orden = 1 ;



INSERT  overwrite TABLE bi_corp_common.dim_prem_premiaciones PARTITION (partition_date)

Select
	mae_prem.id_premiacion 														as  cod_prem_premiacion,
	to_date (mae_prem.fecha_desde) 										as dt_prem_desde,
	to_date (mae_prem.fecha_hasta) 										as dt_prem_hasta,
	to_date (mae_prem.fecha_asignacion) 							as dt_prem_asignacion,
	mae_prem.usuario 																	as ds_prem_premusuario,
	mae_prem.id_objeto_log 														as cod_prem_objetolog,
	mae_prem.descripcion 															as ds_prem_premiacion,
	mae_prem.tabla_novedades 													as ds_prem_novedades,
	mae_prem.id_ejecucion 														as cod_prem_ejecucion,
	mae_prem.descrip_prem_canal 											as de_prem_canal,
	mae_prem.leyenda_prem_canal 											as ds_prem_leyenda,
	mae_soli.id_solicitud 														as cod_prem_solicitud,
	to_date (mae_soli.fecha_alta) 										as dt_prem_solicitudalta,
	to_date (mae_soli.fecha_desde) 										as dt_prem_solicituddesde,
	to_date (mae_soli.fecha_hasta) 										as dt_prem_solicitudhasta,
	mae_soli.periodicidad 														as ds_prem_periodicidad,
	mae_soli.origen_datos 														as ds_prem_origen,
	mae_soli.id_usuario 															as cod_prem_soliusuario,
	case when trim(mae_soli.tipo_interfaz )='null' then null else trim(mae_soli.tipo_interfaz) end cod_prem_interfaz,
	case when trim(mae_soli.codigo_camp  )='null' then null else trim(mae_soli.codigo_camp ) end cod_prem_campania,
	mae_soli.descrip_camp 														as ds_prem_campania,
	case when trim(mae_soli.m_gestion   )='null' then null else trim(mae_soli.m_gestion ) end ds_prem_gestion,
	case when trim(mae_soli.accion_ra   )='null' then null else trim(mae_soli.accion_ra) end ds_prem_accionra,
	mae_soli.id_usuario_peticion 											as ds_prem_usuriopeticion,
	to_date (mae_soli.fecha_ult_modificacion) 				as dt_prem_ultmodificacion,
	case when trim(mae_soli.f_dia  )='null' then null else trim(mae_soli.f_dia ) end ds_prem_dia,
	case when trim(mae_soli.f_semana )='null' then null else trim(mae_soli.f_semana ) end ds_prem_semana,
	case when trim(mae_soli.f_dia_ejec )='null' then null else trim(mae_soli.f_dia_ejec ) end ds_prem_ejecucion,
	case when trim(mae_soli.id_usuario_ult_modificacion )='null' then null else trim(mae_soli.id_usuario_ult_modificacion ) end cod_prem_usuarioultmodificacion,
	mae_soli.sector_auto 															as cod_prem_sectorauto,
	case when trim(mae_soli.sector_ctrl )='null' then null else trim(mae_soli.sector_ctrl ) end cod_prem_sectorcontrol,
	case when trim(mae_soli.estado_auto  )='null' then null else trim(mae_soli.estado_auto ) end ds_prem_estadoauto,
	mae_eve.id_evento 																as cod_prem_evento,
	mae_eve.descripcion 															as ds_prem_evento,
	to_date (mae_eve.fecha_desde) 										as dt_prem_evedesde,
	to_date (mae_eve.fecha_hasta) 										as dt_prem_evehasta,
	mae_eve.descripcion_corta 												as ds_prem_eventocorta,
	mae_prem.partition_date


FROM bi_corp_staging.rio102_prem_maes_premiacion mae_prem

INNER JOIN
	bi_corp_staging.rio102_prem_solicitudes mae_soli
	on mae_prem.id_premiacion = mae_soli.id_premiacion
	and mae_prem.partition_date = mae_soli.partition_date

LEFT JOIN
	bi_corp_common.rel_sol_eve rel_sol_eve
	on mae_soli.id_solicitud = rel_sol_eve.id_solicitud

LEFT JOIN
	bi_corp_staging.rio102_prem_maes_evento mae_eve
	on rel_sol_eve.id_evento = mae_eve.id_evento
	and mae_prem.partition_date = mae_eve.partition_date



WHERE mae_prem.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_Premiaciones-Daily_catchup2020') }}';

