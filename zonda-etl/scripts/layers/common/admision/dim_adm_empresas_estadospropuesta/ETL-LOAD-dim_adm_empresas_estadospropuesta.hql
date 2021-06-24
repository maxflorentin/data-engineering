set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_empresas_estadospropuesta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    cod_Estado as cod_adm_estado,
    lower(des_Estado) as ds_adm_estado
from bi_corp_staging.sge_cod_estado_net
where partition_Date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
    and (NOT (cod_tramite ='F487')
    AND NOT (cod_Estado IN ('RR') AND UPPER(des_estado) ='ERROR EN ANTEC. INTERNOS')
    AND NOT (cod_Estado IN ('A','B','PG') AND COD_TRAMITE='G_BMA')
    AND NOT (cod_Estado IN ('PC','PR','V','X') AND COD_TRAMITE='ATRI')
    AND NOT  (cod_tramite ='G_BMA_F487' AND COD_ESTADO IN ('A','B','D','E','G','H','I','J','K','M','N','O','P','Q'))
)
