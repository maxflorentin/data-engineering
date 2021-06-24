SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS max_partition_producto;
CREATE TEMPORARY TABLE max_partition_producto AS
SELECT cod_prod, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_producto
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
group by cod_prod;


INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_producto
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_prod) cod_rec_producto ,
	trim(desc_prod) ds_rec_produto ,
	trim(est_prod) cod_rec_estado_producto,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad , a.cod_prod , UPPER(desc_prod) desc_prod , est_prod , partition_date
	FROM
		bi_corp_staging.rio56_producto a
    inner join max_partition_producto b
    on a.cod_prod=b.cod_prod
    and a.partition_date=b.max_partition_date
    ) A;
