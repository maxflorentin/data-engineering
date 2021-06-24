set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Calculamos la maxima particion
DROP TABLE IF EXISTS max_partition_concepto;
CREATE TEMPORARY TABLE max_partition_concepto AS
SELECT cod_cpto, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_concepto
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
group by cod_cpto;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_concepto
SELECT
	trim(cod_entidad) cod_rec_entidad ,
	trim(cod_cpto) cod_rec_concepto ,
	trim(desc_cpto) ds_rec_concepto ,
	case when trim(desc_detall_cpto)='null' then null else trim(desc_detall_cpto) end AS ds_rec_detalle_concepto ,
	--trim(desc_detall_cpto) ds_rec_detalle_concepto ,
	trim(est_cpto) cod_rec_estado_concepto,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad , a.cod_cpto , desc_cpto , desc_detall_cpto , est_cpto ,partition_date
	FROM
		bi_corp_staging.rio56_concepto a
    inner join 	max_partition_concepto b
    on a.cod_cpto=b.cod_cpto
    and a.partition_date=b.max_partition_date
    	) A;