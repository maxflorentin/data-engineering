"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_ivrderivacionhabilidades
PARTITION (partition_date)

SELECT
DISTINCT SUBSTRING(fecha,1,19)   															TS_CC_FECHA,
cast(nup as BIGINT)     															COD_PER_NUP,
CASE WHEN trim(connectionid)='NULL' or connectionid='null'
	 THEN NULL ELSE trim(connectionid) END 											COD_CC_CONEXION,
CASE WHEN trim(sesion)='NULL' or sesion='null'
	 THEN NULL ELSE trim(sesion) END												COD_CC_SESION,
CASE WHEN trim(vq)='NULL' or vq='null'
	 THEN NULL ELSE trim(vq) END													DS_CC_VQ,
CASE WHEN trim(grupo_agentes)='NULL' or grupo_agentes='null'
	 THEN NULL ELSE trim(grupo_agentes) END											COD_CC_GRUPO_AGENTES,
CASE WHEN trim(grupo_agentes_des)='NULL' or grupo_agentes_des='null'
	 THEN NULL ELSE trim(grupo_agentes_des) END 									DS_CC_GRUPO_AGENTES,
CASE WHEN trim(habilidad_1)='NULL' or habilidad_1='null'
	 THEN NULL ELSE trim(habilidad_1) END 											DS_CC_HABILIDAD1,
CASE WHEN trim(habilidad_2)='NULL' or habilidad_2='null'
	 THEN NULL ELSE trim(habilidad_2) END 											DS_CC_HABILIDAD2,
CASE WHEN trim(agente)='NULL' or agente='null'
	 THEN NULL ELSE upper(trim(agente)) END 												COD_CC_LEGAJO,
CASE WHEN trim(descrip)='NULL' or descrip='null'
	 THEN NULL ELSE trim(descrip) END 												DS_CC_DERIVACION,
CASE WHEN trim(cod_app)='NULL' or cod_app='null'
	 THEN NULL ELSE trim(cod_app) END 												COD_CC_CODIGOAPP,
partition_date																		PARTITION_DATE


from bi_corp_staging.rio32_histlog_derivacion_habilidades
WHERE partition_date= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"

"