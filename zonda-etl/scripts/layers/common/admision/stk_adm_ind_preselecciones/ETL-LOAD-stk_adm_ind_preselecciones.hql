set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_preselecciones
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
trim(e.cod_promocion) as ds_adm_codpromocion,
trim(e.tpo_doc) as ds_adm_tpodoc,
cast(e.nro_doc as bigint) as fc_adm_nrodoc,
trim(e.fec_vigencia) as ts_adm_fecvigencia,
trim(e.mar_sexo) as ds_mar_sexo,
cast(e.mon_ingresos as bigint) as fc_adm_moningresos,
trim(e.cod_prod_ofertado) as cod_prod_ofertado,
trim(e.cui_batch) as ds_adm_cuibatch,
cast(e.score_veraz_preseleccion as bigint) as fc_adm_scoreverazpreseleccion,
cast(e.score_triad as bigint) as fc_adm_scoretriad,
cast(e.asistencia_visa as bigint) as fc_adm_asistenciavisa,
cast(e.asistencia_amex as bigint) as fc_adm_asistenciaamex,
cast(e.asistencia_ac as bigint) as fc_adm_asistenciaac,
cast(e.asistencia_pp as bigint) as fc_adm_asistenciapp,
cast(e.debt_burden as bigint) as fc_adm_debtburden,
cast(e.afectacion as bigint) as fc_adm_afectacion,
cast(e.asistencia_master as bigint) as fc_adm_asistenciamaster,
cast(e.asistencia_visabusiness as bigint) as fc_adm_asistenciavisabusiness,
cast(e.asistencia_pmomono as bigint) as fc_adm_asistenciapmomono,
cast(e.asistencia_cesionch as bigint) as fc_adm_asistenciacesionch,
cast(e.cuota_prestamo as bigint) as fc_adm_cuotaprestamo,
cast(e.valor_cuotas_prestamo as bigint) as fc_adm_valorcuotasprestamo,
trim(e.cod_prod) as cod_prod_producto
from bi_corp_staging.alcen_tpreseleccion e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;