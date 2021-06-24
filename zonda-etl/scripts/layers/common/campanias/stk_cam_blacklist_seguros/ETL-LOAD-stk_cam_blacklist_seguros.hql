set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.stk_cam_blacklist_seguros
PARTITION(partition_date)
SELECT
    a.cacn_cd_nacionalidad cod_cam_tipodoc,
    cast(a.cacn_nu_cedula_rif as BIGINT) ds_cam_nrodoc,
    a.cacn_nm_apellido_razon ds_cam_nombre,
    IF(b.rio35_cli_nup is null, cast(a.cacn_nu_nup as BIGINT), cast(b.rio35_cli_nup as BIGINT)) cod_per_nup,
    a.partition_date
FROM bi_corp_staging.rio6_cart_clientes a
LEFT JOIN bi_corp_staging.rio6_batb_datos_rio35 b
ON (b.rio6_cacn_cd_nacionalidad = a.cacn_cd_nacionalidad
    AND b.rio6_cacn_nu_cedula_rif = a.cacn_nu_cedula_rif
    AND b.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}')
WHERE a.cacn_st_estado = '3'
    AND a.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CAMPANIAS') }}'
ORDER BY cod_cam_tipodoc, ds_cam_nrodoc;