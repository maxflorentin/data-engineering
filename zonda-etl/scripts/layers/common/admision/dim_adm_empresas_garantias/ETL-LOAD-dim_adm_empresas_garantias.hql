set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_empresas_garantias
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    pecdgent as ds_adm_pecdgent,
    peidioma as ds_adm_peidioma,
    cod_gartia as cod_adm_gartia,
    des_gartia as ds_adm_des_gartia,
    cast(porc_max as decimal(16,2)) as dec_adm_porc_max,
    peusualt as ds_adm_peusualt,
    peusumod as ds_adm_peusumod,
    pefecalt as dt_adm_pefecalt,
    pefecmod as dt_adm_pefecmod,
    cast(puntaje as int) as int_adm_puntaje,
    categoria as ds_adm_categoria,
    tipo as ds_adm_tipo,
    cast(grupo_gartia as int) as int_adm_grupo_gartia,
    des_garantia_bma as ds_adm_des_garantia_bma,
    gtia_clean as ds_adm_gtia_clean
FROM bi_corp_staging.sge_garantias
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';