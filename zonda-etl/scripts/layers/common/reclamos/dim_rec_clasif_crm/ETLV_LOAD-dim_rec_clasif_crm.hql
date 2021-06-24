INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_clasif_crm
SELECT
	trim(COD_ENTIDAD) cod_rec_entidad ,
	trim(TPO_PERS) cod_rec_tipo_persona ,
	trim(COD_CRM) cod_rec_crm ,
	UPPER(DESC_CRM) ds_rec_crm ,
	trim(ID_CLASIF_SELECT) cod_rec_clasif_select ,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		COD_ENTIDAD , TPO_PERS , COD_CRM , DESC_CRM , ID_CLASIF_SELECT , MAX(PARTITION_DATE) ULT_PARTITION_DATE
	FROM
		BI_CORP_STAGING.RIO56_CLASIF_CRM
	GROUP BY
		COD_ENTIDAD , TPO_PERS , COD_CRM , DESC_CRM , ID_CLASIF_SELECT ) A;