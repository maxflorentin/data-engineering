set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_estados
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
concat(lpad(e.cod_sucursal, 3, '0') ,lpad(e.nro_solicitud, 17, '0')) as cod_adm_tramite,
cast(e.cod_sucursal as int) as cod_suc_sucursal,
cast(e.nro_solicitud as bigint) as cod_adm_nrosolicitud,
trim(e.cod_canal) as cod_adm_canal,
trim(e.fec_ingreso_rio2) as ts_adm_fecingresorio2,
trim(e.fec_envio_sw1) as ts_adm_fecenviosw1,
trim(e.fec_envio_sw2) as ts_adm_fecenviosw2,
trim(e.cod_estado_sw) as cod_adm_estadosw,
trim(e.fec_desicion_sw) as ts_adm_fecdesicionsw,
trim(e.mar_pedido_veraz) as ds_adm_marpedidoveraz,
trim(e.mar_reutiliza_veraz) as ds_adm_marreutilizaveraz,
trim(e.fec_envio_veraz) as ts_adm_fecenvioveraz,
trim(e.fec_recep_veraz) as ts_adm_fecrecepveraz,
trim(e.fec_ingreso_srs) as ts_adm_fecingresosrs,
trim(e.fec_asig_ana_srs) as ts_adm_fecasiganasrs,
trim(e.fec_ini_resol_srs) as ts_adm_feciniresolsrs,
trim(e.fec_fin_resol_srs) as ts_adm_fecfinresolsrs,
trim(e.fec_retro_srs) as ts_adm_fecretrosrs,
trim(e.fec_reasig_ana_srs) as ts_adm_fecreasiganasrs,
trim(e.cod_estado_srs) as cod_adm_estadosrs,
trim(e.fec_resol_suc) as ts_adm_fecresolsuc,
trim(e.cod_resol_suc) as cod_adm_resolsuc,
trim(e.fec_resol_altas) as ts_adm_fecresolaltas,
trim(e.cod_resol_altas) as cod_adm_resolaltas,
trim(e.fec_retro) as ts_adm_fecretro,
trim(e.cod_estado_retro) as cod_adm_estadoretro,
trim(e.cod_estado_actual) as cod_adm_estadoactual,
trim(e.fec_estado_actual) as ts_adm_fecestadoactual,
trim(e.cod_estado_asol) as cod_adm_estado_asol,
trim(e.fec_estado_asol) as ts_adm_fecestadoasol,
trim(e.nom_sector_altas) as ds_adm_nomsectoraltas
from bi_corp_staging.alcen_testado e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;