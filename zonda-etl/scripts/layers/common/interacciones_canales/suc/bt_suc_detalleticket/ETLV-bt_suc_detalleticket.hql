set mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_detalleticket PARTITION (partition_date)
--------------------------------------------------
SELECT from_unixtime(unix_timestamp(TRIM(ET.fecha), 'yyyyMMdd'),"yyyy-MM-dd") dt_suc_atencion
	, TRIM(ET.sucursal) cod_suc_sucursal
	, TRIM(ET.zona) cod_suc_zona
	, TRIM(UPPER(ET.tipo_cola)) ds_suc_tipocola
	, TRIM(ET.ticket) cod_suc_ticket
	, TRIM(ET.numero_servicio) int_suc_servicio
	, TRIM(ET.nombre_servicio) ds_suc_servicio
	, TRIM(ET.numero_puesto) int_suc_puesto
	, IF(TRIM(ET.nombre_puesto) = '', NULL, TRIM(ET.nombre_puesto)) ds_suc_puesto
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 1, 7), NULL) cod_suc_legajo
	, IF(SUBSTRING(TRIM(ET.usuario), 8, 1) = ' ', SUBSTRING(TRIM(ET.usuario), 9, 42), NULL) ds_suc_nombreusuario
	, TRIM(ET.t_obj_espera) ts_suc_objespera
	, TRIM(ET.t_max_espera) ts_suc_maxespera
	, TRIM(ET.fecha_ingreso) ts_suc_ingreso
	, TRIM(ET.fecha_llamado) ts_suc_llamado 
	, TRIM(ET.fecha_fin_atencion) ts_suc_finatencion
	, TRIM(ET.tiempo_espera) ts_suc_espera
	, TRIM(ET.tiempo_atencion) ts_suc_tiempoatencion
	, IF(TRIM(ET.auxiliar1) = '', NULL, TRIM(ET.auxiliar1)) ds_suc_auxiliar1
	, IF(TRIM(ET.auxiliar2) = '', NULL, TRIM(ET.auxiliar2)) ds_suc_auxiliar2
	, IF(TRIM(ET.cod_motivo) = '', NULL, TRIM(ET.cod_motivo)) cod_suc_motivovisita
	, IF(TRIM(ET.nom_motivo) = '', NULL, TRIM(UPPER(ET.nom_motivo))) ds_suc_motivovisita
	, TRIM(ET.estado) cod_suc_estado
	, IF(TRIM(ET.motivo_abandono) = '', NULL, TRIM(ET.motivo_abandono)) ds_suc_motivoabandono
	, TRIM(ET.tecn) int_suc_tecn 
	, ET.partition_date 
FROM bi_corp_staging.nesr_encolador_det_ticket ET 
WHERE ET.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;