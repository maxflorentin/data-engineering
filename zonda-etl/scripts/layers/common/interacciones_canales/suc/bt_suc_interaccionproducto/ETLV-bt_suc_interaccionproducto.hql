SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions=1300;
set hive.exec.max.dynamic.partitions.pernode=1300;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionproducto PARTITION (partition_date)

SELECT DISTINCT TRIM(IP.cd_interaccion_producto) cod_suc_interaccionproducto
	, TRIM(IP.cd_interaccion) cod_suc_interaccion
	, CAST(IP.id_acc AS INT) cod_suc_acc
	, TRIM(IP.ds_campana_o_producto) ds_suc_campanaoproducto 
	, IF(TRIM(IP.mc_campana) = 'S',1,0) flag_suc_campana
	, CAST(IP.cd_escenario AS INT) cod_suc_escenario
	, CAST(IP.cd_envio AS INT) cod_suc_envio
	, TRIM(IP.ds_producto_crm) ds_suc_productocrm
	, CAST(IP.cd_gestion AS INT) cod_suc_gestion
	, CAST(IP.cd_resultado AS INT) cod_suc_resultado
	, TRIM(IP.json) ds_suc_json
	, TRIM(IP.ds_comentario) ds_suc_comentario
	, IF(TRIM(IP.mc_envio_mail) = 'S',1,0) flag_suc_enviomail
	, to_date(IP.dt_agenda_horario) dt_suc_agendahorario
	, SUBSTRING(IP.dt_agenda_horario, 1, 19) ts_suc_agendahorario
	, to_date(IP.dt_modificacion) dt_suc_modificacion
	, SUBSTRING(IP.dt_modificacion, 1, 19) ts_suc_modificacion
	, to_date(IP.dt_gestion) dt_suc_gestion
	, SUBSTRING(IP.dt_gestion, 1, 19) ts_suc_gestion
	, CAST(IP.cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, TRIM(IP.cd_producto_crm) cod_suc_productocrm
	, CAST(IP.vl_nro_solicitud AS INT) cod_suc_vlnumerosolicitud
	, TRIM(IP.cd_canal_solicitud) cod_suc_canalsolicitud
	, TRIM(IP.id_camp_buc) cod_suc_campbuc
	, TRIM(IP.ds_contacto) ds_suc_contacto
	, IF(TRIM(IP.seguimientotec) = 'S',1,0) flag_suc_seguimientotec
	, TRIM(UPPER(GE.grupo)) ds_suc_gestion 
	, TRIM(UPPER(GE.descripcion)) ds_suc_motivo
	, IP.partition_date
FROM bi_corp_staging.rio151_tbl_interaccion_producto IP
LEFT JOIN bi_corp_staging.rio151_tbl_combo_iatx_tmk GE 
	ON TRIM(IP.cd_gestion) = TRIM(GE.campo1)
	AND TRIM(IP.cd_identificacion_resultado) = TRIM(GE.campo3)
	AND GE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio151_tbl_combo_iatx_tmk', dag_id='LOAD_CMN_Sucursales') }}'
WHERE IP.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;