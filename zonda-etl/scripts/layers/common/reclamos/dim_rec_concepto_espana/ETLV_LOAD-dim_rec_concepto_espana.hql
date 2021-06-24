set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Calculamos la maxima particion
DROP TABLE IF EXISTS max_partition_concepto;
CREATE TEMPORARY TABLE max_partition_concepto AS
SELECT cod_cpto, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_gc_conceptos_espana
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
group by cod_cpto;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_concepto_espana
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_cpto) cod_rec_concepto_espana ,
	trim(desc_cpto) ds_rec_concepto_espana ,
	trim(cod_tipo_reclamo) cod_rec_tipo_reclamo_espana ,
	trim(estado) cod_rec_estado_concepto_espana,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
		SELECT
			cod_entidad , a.cod_cpto , desc_cpto , cod_tipo_reclamo , estado ,partition_date
		FROM
			bi_corp_staging.rio56_gc_conceptos_espana a
    		inner join 	max_partition_concepto b
    			on a.cod_cpto=b.cod_cpto
    			and a.partition_date=b.max_partition_date
    ) A;