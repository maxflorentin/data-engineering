SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT DISTINCT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, TRIM(INH.cod_caus) cod_cys_causal
	, TRIM(CA.ds_caus) ds_cys_causal
	, TRIM(INH.sec_caus) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, TRIM(INH.fec_rehb) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, IF(TRIM(INH.vigencia) = 'S', datediff(to_date(INH.partition_date), to_date(INH.fec_inh)), NULL) int_cys_antiguedad
	, INH.partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
LEFT JOIN bi_corp_staging.afir_cod_causales CA 
	ON TRIM(INH.cod_caus) = TRIM(CA.cod_caus)
WHERE INH.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}' ;