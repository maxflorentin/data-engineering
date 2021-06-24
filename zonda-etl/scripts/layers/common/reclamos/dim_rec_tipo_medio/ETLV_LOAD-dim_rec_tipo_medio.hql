INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_tipo_medio
SELECT
	trim(tpo_medio) cod_rec_tipo_medio ,
	trim(desc_medio) ds_rec_tipo_medio ,
	trim(desc_detall_medio) ds_rec_detall_tipo_medio ,
	trim(indi_tpo_medio) cod_rec_indi_tipo_medio ,
	trim(est_medio) cod_rec_est_medio ,
	trim(sector_owner) ds_rec_sector_owner_medio ,
	case when indi_visible='S' then 1 else 0 end flag_rec_visible_medio ,
	CASE WHEN lower(indi_msj_asoc) = 'null' then NULL ELSE indi_msj_asoc END cod_rec_mjs_asoc_medio ,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		tpo_medio , desc_medio , desc_detall_medio , indi_tpo_medio , est_medio , sector_owner , indi_visible , indi_msj_asoc , max(partition_date) ult_partition_date
	FROM
		bi_corp_staging.rio56_tipo_medios
	GROUP BY
		tpo_medio , desc_medio , desc_detall_medio , indi_tpo_medio , est_medio , sector_owner , indi_visible , indi_msj_asoc ) A;

