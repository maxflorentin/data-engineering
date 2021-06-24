SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT 	overwrite TABLE bi_corp_common.stk_suc_zonas
PARTITION (partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}')
SELECT
    zon.cod_suc_zona,
	cast(NULL as string) ds_suc_zona,
    cast(NULL as string) ds_suc_gerente,
    IF(suc.habilitado = '1', 'ABIERTA', 'CERRADA') ds_suc_estado,
    CAST(suc.ref_legacy AS INT) cod_suc_sucursal,
    cast(NULL as string) dt_suc_fechaalta,
	cast(NULL as string) dt_suc_fechamodif,
	cast(NULL as string) dt_suc_fechabaja
FROM bi_corp_staging.rio350_sucursales suc
LEFT JOIN bi_corp_staging.rio350_rel_sucursal_zona zon
	ON zon.cod_suc_sucursal = CAST(suc.ref_legacy AS INT)
WHERE suc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;
