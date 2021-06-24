set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_estadossolicitud
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
concat(lpad(e.cod_sucursal, 3, '0') ,lpad(e.nro_solicitud, 17, '0')) as cod_adm_tramite,
cast(e.cod_sucursal as int) as cod_suc_sucursal,
cast(e.nro_solicitud as bigint) as cod_adm_nrosolicitud,
cast(e.cod_estado as int) as cod_adm_estado,
e.fec_estado as ts_adm_estado,
e.hra_estado as ts_adm_hraestado,
e.cod_usuario as cod_adm_usuario,
e.cod_sector_usu as cod_adm_sectorusu,
cast(e.nro_cta as bigint) as cod_nro_cuenta,
cast(e.lim_visa as bigint) as fc_adm_limvisa,
cast(e.lim_acte as bigint) as fc_adm_limacte,
cast(e.lim_ppp as bigint) as fc_adm_limppp,
cast(e.lim_amex as bigint) as fc_adm_limamex,
cast(e.lim_cheq as bigint) as fc_adm_limcheq,
e.lim_ptmo_mon as fc_adm_limptmomon,
e.origen as ds_adm_origen
from bi_corp_staging.alcen_testado_asol e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;