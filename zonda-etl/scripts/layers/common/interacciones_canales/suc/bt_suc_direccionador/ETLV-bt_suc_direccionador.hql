set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode = nonstrict;

INSERT overwrite TABLE bi_corp_common.bt_suc_direccionador PARTITION (partition_date)

SELECT
	coalesce(from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd"),from_unixtime(unix_timestamp(TRIM(ED.fechaticket), 'yyyyMMddHHmmss'),"yyyy-MM-dd")) dt_suc_fecha_atencion,
	TRIM(ED.id_tkt) cod_suc_ticket,
	CAST(ED.id_suc AS INT) cod_suc_sucursal,
	TRIM(ET.zona) cod_suc_zona,
	TRIM(UPPER(ET.tipo_cola)) ds_suc_tipocola,
	UPPER(TRIM(ED.sector_espera)) ds_suc_sector,
	TRIM(ED.apellidocliente) ds_per_apellido,
	TRIM(ED.nombrecliente) ds_per_nombre,
	TRIM(ED.cod_producto) cod_suc_producto,
	IF(TRIM(ET.cod_motivo) = '', NULL, TRIM(ET.cod_motivo)) cod_suc_motivovisita,
	IF(TRIM(ET.nom_motivo) = '', NULL, TRIM(UPPER(ET.nom_motivo))) ds_suc_motivovisita,
	TRIM(ED.mejorproducto) ds_suc_mejorproducto,
	TRIM(ED.segmento) cod_suc_segmento,
	TRIM(ED.semafororentabilidad) cod_suc_semafororentabilidad,
	TRIM(ED.semafororenta) cod_suc_semafororenta,
	TRIM(ED.productosegmento) cod_suc_productosegmento,
	TRIM(ED.contacto) cod_suc_contacto,
	DS.ds_tipodoc ds_per_tipodoc,
	CAST(ED.nrodoc AS BIGINT) ds_per_numdoc,
	TRIM(ED.subsegmento) cod_suc_subsegmento,
	CAST(ED.nup AS BIGINT) cod_per_nup,
	from_unixtime(unix_timestamp(TRIM(ED.fechaticket), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_ticket,
	TRIM(ED.campana) cod_suc_campana,
	CAST(ED.numero_servicio AS INT) int_suc_servicio,
	TRIM(ED.tipoatencion) cod_suc_tipoatencion,
	TRIM(ED.legajo) cod_suc_legajo,
	TRIM(ED.canalcomp) cod_suc_canalcomp,
	TRIM(ED.id_gestion) cod_suc_gestion,
	from_unixtime(unix_timestamp(TRIM(ED.fecha_proceso), 'yyyyMMddHHmmss'),"yyyy-MM-dd HH:mm:ss") ts_suc_proceso,
	TRIM(ED.procesado) cod_suc_procesado,
	TRIM(ED.nombre_servicio) ds_suc_servicio,
	TRIM(ED.oficial_atencion) cod_suc_oficialatencion,
	TRIM(ED.oficial_nombre) ds_suc_oficialnombre,
	TRIM(ET.numero_puesto) as cod_suc_nropuesto,
	IF(TRIM(ET.nombre_puesto) = '', NULL, TRIM(ET.nombre_puesto)) ds_suc_puesto,
	TRIM(ED.origen) cod_suc_origen,
	TRIM(ED.estado) ds_suc_estado,
	IF (TRIM(ET.t_obj_espera) = '', NULL, TRIM(ET.t_obj_espera)) ds_suc_objespera,
	round(((cast(SUBSTRING(trim(ET.t_obj_espera),1,2) as bigint))*3600)+
			(cast(SUBSTRING(trim(ET.t_obj_espera),4,2) as bigint)*60)+
			(cast(SUBSTRING(trim(ET.t_obj_espera),7,2) as bigint)),1)
	as fc_suc_objespera_seg,
	IF (TRIM(ET.t_obj_espera) = '', NULL, TRIM(ET.t_max_espera)) ds_suc_maxespera,
	round(((cast(SUBSTRING(TRIM(ET.t_max_espera),1,2) as bigint))*3600)+
			(cast(SUBSTRING(TRIM(ET.t_max_espera),4,2) as bigint)*60)+
			(cast(SUBSTRING(TRIM(ET.t_max_espera),7,2) as bigint)),1)
	as fc_suc_maxespera_seg,
	CONCAT (from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd "), TRIM(ET.fecha_ingreso)) ts_suc_ingreso,
	CONCAT (from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd "), TRIM(ET.fecha_llamado)) ts_suc_llamado,
	CONCAT (from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd "), TRIM(ET.fecha_fin_atencion)) ts_suc_finatencion,
	IF (TRIM(ET.t_obj_espera) = '', NULL, TRIM(ET.tiempo_espera)) ds_suc_espera,
	round(((cast(SUBSTRING(trim(ET.tiempo_espera),1,2) as bigint))*3600)+
			(cast(SUBSTRING(trim(ET.tiempo_espera),4,2) as bigint)*60)+
			(cast(SUBSTRING(trim(ET.tiempo_espera),7,2) as bigint)),1)
	as  fc_suc_espera_seg,
	IF (TRIM(ET.t_obj_espera) = '', NULL, TRIM(ET.tiempo_atencion)) ds_suc_tiempoatencion,
	round(((cast(SUBSTRING(trim(ET.tiempo_atencion),1,2) as bigint))*3600)+
			(cast(SUBSTRING(trim(ET.tiempo_atencion),4,2) as bigint)*60)+
			(cast(SUBSTRING(trim(ET.tiempo_atencion),7,2) as bigint)),1)
	as fc_suc_tiempoatencion_seg,
	TRIM(UPPER(ED.motivo_abandono)) ds_suc_motivoabandono,
	CAST(ED.tecn AS BIGINT) int_suc_tecn,
	ED.partition_date
FROM bi_corp_staging.nesr_encolador_d ED
LEFT JOIN bi_corp_staging.nesr_encolador_det_ticket ET
		on(trim(ED.id_tkt)=TRIM(ET.ticket)
	   		and  CAST(ED.id_suc AS INT)= cast(TRIM(ET.sucursal) as int)
	   		and ET.partition_date = ED.partition_date )
LEFT JOIN
        (SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
         FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113'
         AND partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Sucursales') }}') DS
        ON TRIM(ED.tipodoc) = DS.tipodoc
WHERE ED.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;