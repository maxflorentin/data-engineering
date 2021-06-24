set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_estadosolicitudriesgos
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
concat(lpad(e.cod_sucursal, 3, '0') ,lpad(e.nro_solicitud, 17, '0')) as cod_adm_tramite,
cast(cod_sucursal as int) as cod_suc_sucursal,
cast(nro_solicitud as bigint) as cod_adm_nrosolicitud,
trim(e.cod_legajo) as cod_adm_legajo,
trim(e.fecha) as ts_adm_fecha,
trim(e.hora) as ts_adm_hora ,
cast(e.cod_estado as bigint) as cod_adm_estado,
trim(t.des_estado) as ds_adm_estado ,
trim(t.cod_sector) as cod_adm_sector,
trim(e.cod_usuario) as cod_adm_usuario,
trim(e.des_obs_estado) as ds_adm_obs_estado,
trim(e.fec_ini_resol) as dt_adm_feciniresol ,
trim(e.fec_fin_resol) as dt_adm_fecfinresol ,
trim(e.mot_resol1) as ds_adm_motresol1,
trim(e.mot_resol2) as ds_adm_motresol2,
trim(e.mot_resol3) as ds_adm_motresol3,
trim(e.mot_resol4) as ds_adm_motresol4,
trim(e.mot_resol5) as ds_adm_motresol5,
trim(e.mot_ingreso_sup1) as ds_adm_motingresosup1,
trim(e.mot_ingreso_sup2) as ds_adm_motingresosup2,
trim(e.mot_ingreso_sup3) as ds_adm_motingresosup3,
trim(e.mot_ingreso_sup4) as ds_adm_motingresosup4,
trim(e.mot_ingreso_sup5) as ds_adm_motingresosup5,
trim(e.mot_recalificacion) as ds_adm_motrecalificacion,
cast(e.sec_estado as int) as ds_adm_secestado
from bi_corp_staging.cvc_tsol_estado e
left join bi_corp_staging.cvc_testado t on e.cod_estado = t.cod_estado and t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;