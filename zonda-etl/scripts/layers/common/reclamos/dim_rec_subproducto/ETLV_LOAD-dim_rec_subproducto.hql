SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS max_partition_subproducto;
CREATE TEMPORARY TABLE max_partition_subproducto AS
SELECT cod_subprod, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_subproducto
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
group by cod_subprod;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_subproducto
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_subprod) cod_rec_subproducto ,
	UPPER(desc_subprod) ds_rec_subproducto ,
	trim(est_subprod) ds_rec_estado_subproducto,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad , a.cod_subprod , desc_subprod , est_subprod , partition_date
	FROM
		bi_corp_staging.rio56_subproducto a
    inner join max_partition_subproducto b
    on a.cod_subprod=b.cod_subprod
    and a.partition_date=b.max_partition_date
    ) A;

