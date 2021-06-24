SET mapred.job.queue.name=root.dataeng;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_concepto_bcra_circ
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_cpto) cod_rec_concepto_bcra ,
	trim(ide_circuito) cod_rec_circuito ,
	trim(estado) cod_rec_estado_concepto_bcra,
	'{{ ti.xcom_pull(task_ids=' InputConfig', key=' partition_date', dag_id=' LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad , cod_cpto , ide_circuito , estado , max(partition_date) ult_partition_date
	FROM
		bi_corp_staging.rio56_gc_conceptos_bcra_circ
	GROUP BY
		cod_entidad , cod_cpto , ide_circuito , estado ) A;