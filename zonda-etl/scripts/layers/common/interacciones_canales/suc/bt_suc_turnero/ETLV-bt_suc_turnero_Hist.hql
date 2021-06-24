SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_turnero PARTITION (partition_date)
--------------------------------------------------
SELECT CAST(ET.id_turno AS BIGINT) cod_suc_turno
	, CAST(ET.id_estado AS INT) cod_suc_estado
	, TRIM(EE.descri) ds_suc_estado
	, DS.ds_tipodoc ds_suc_tipodoc
	, IF(TRIM(ET.nrodoc) = 'null', NULL, CAST(ET.nrodoc AS BIGINT)) ds_per_numdoc
	, IF(TRIM(ET.sector) = 'P', 'PLATAFORMA', IF(TRIM(ET.sector) = 'C', 'CAJA', NULL)) ds_suc_sector
	, CAST(ET.id_suc AS INT) cod_suc_sucursal
	, TRIM(SUC.descri) ds_suc_sucursal
	, ET.fecha_hora_desde dt_suc_fechahoradesde
	, ET.fecha_hora_hasta dt_suc_fechahorahasta
	, TRIM(ET.fraccion) ds_suc_fraccion
	, TRIM(ET.dia) ds_suc_dia
	, IF(TRIM(ET.nup) = 'null', NULL, CAST(ET.nup AS BIGINT)) cod_per_nup
	, IF(TRIM(ET.apellido) = 'null', NULL, TRIM(ET.apellido)) ds_per_apellido
	, IF(TRIM(ET.nombre) = 'null', NULL, TRIM(ET.nombre)) ds_per_nombre
	, IF(TRIM(ET.pesubseg) = 'null', NULL, TRIM(ET.pesubseg)) cod_suc_subseg
	, IF(TRIM(ET.pesexper) = 'null', NULL, TRIM(ET.pesexper)) cod_per_sexo
	, CAST(ET.id_motivo AS INT) cod_suc_motivovisita
	, TRIM(MV.descripcion) ds_suc_motivovisita
	, IF(TRIM(ET.mail) = 'null', NULL, TRIM(ET.mail)) ds_suc_mail
	, IF(TRIM(ET.cuit) = 'null', NULL, CAST(ET.cuit AS BIGINT)) ds_per_cuit
	, IF(TRIM(ET.id_turno_orig) = 'null', NULL, CAST(ET.id_turno_orig AS INT)) cod_suc_turnooriginal
	, IF(TRIM(ET.id_ejec_atencion) = 'null', NULL, TRIM(ET.id_ejec_atencion)) cod_suc_ejecatencion
	, IF(TRIM(ET.id_suc_atencion) = 'null', NULL, TRIM(ET.id_suc_atencion)) cod_suc_sucursalatencion
	, TRIM(ET.id_puesto_citas) cod_suc_puestocitas
	, IF(TRIM(ET.telefono) = 'null', NULL, TRIM(ET.telefono)) ds_suc_telefono
	, TRIM(ET.id_tipo_turno) cod_suc_tipoturno
	, IF(TRIM(ET.id_tipo_turno) = '1', 'FISICO', IF(TRIM(ET.id_tipo_turno) = '2', 'REMOTO', NULL)) ds_suc_tipoturno
	, case WHEN SUBSTRING(ET.fecha_desde, 1, 19) = 'null' then ET.fecha_hora_desde ELSE ET.fecha_desde end ts_suc_fechadesde
	, case WHEN SUBSTRING(ET.fecha_hasta, 1, 19) = 'null' then ET.fecha_hora_desde ELSE ET.fecha_hasta end ts_suc_fechahasta
	, EX.id_canal as cod_suc_canal
	, EX.fecha_alta as dt_suc_fechaalta
	, EX.comentario_cliente as ds_suc_comentariocliente
	, case WHEN SUBSTRING(ET.fecha_hasta, 1, 10) = 'null' then from_unixtime(unix_timestamp(SUBSTRING(ET.fecha_hora_desde,1,8),'yyyyMMdd'), 'yyyy-MM-dd') ELSE SUBSTRING(ET.fecha_hasta, 1, 10) end partition_date
FROM bi_corp_staging.rio154_enc_turno ET
-------------------------------------------------------ESTADO
LEFT JOIN bi_corp_staging.rio154_enc_estado EE
	ON  EE.id_estado = ET.id_estado
	AND EE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales-History') }}'
-------------------------------------------------------
LEFT JOIN bi_corp_staging.rio154_enc_turno_ext EX
	ON  EX.id_turno = ET.id_turno
	AND EX.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales-History') }}'
-------------------------------------------------------MOTIVO
LEFT JOIN bi_corp_staging.rio154_motivo_visita_v2 MV
	ON MV.id_motivo = ET.id_motivo
	AND MV.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales-History') }}'
-------------------------------------------------------SUCURSAL
LEFT JOIN bi_corp_staging.rio154_sucursalrio SUC
	ON SUC.codigo  = ET.id_suc
	AND SUC.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales-History') }}'
-------------------------------------------------------DS_TIPODOC
LEFT JOIN
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113'AND partition_date = '2021-05-05') DS
	ON TRIM(ET.tipodoc) = DS.tipodoc
------------------------------------------------------
WHERE ET.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales-History') }}' ;