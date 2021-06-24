set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_estadocvcsri
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
trim(cod_sector) as cod_adm_sector,
cast(cod_sucursal as int) as cod_suc_sucursal,
cast(nro_solicitud as bigint) as cod_adm_nrosolicitud,
trim(fec_ingreso) as ts_adm_fecingreso,
trim(fec_ini_resol) as ts_adm_feciniresol,
trim(fec_fin_resol) as ts_adm_fecfinresol,
trim(cod_estado) as cod_adm_estado,
trim(mot_resol1) as ds_adm_motresol1,
trim(mot_resol2) as ds_adm_motresol2,
trim(mot_resol3) as ds_adm_motresol3,
trim(mot_resol4) as ds_adm_motresol4,
trim(mot_resol5) as ds_adm_motresol5,
trim(cod_analista) as cod_adm_analista
from bi_corp_staging.alcen_vestado_cvcsri e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;
