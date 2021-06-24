set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_resumen_crediticio
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(r.nro_prop as int) as cod_adm_numero_propuesta,
    cast(r.penumper as int) as cod_adm_numero_persona_sge,
    cast(r.anio_ganancias as int) as int_adm_anio_ganancias,
    cast(r.imp_ingresos as decimal(30, 2)) as fc_adm_total_ingresos_gravados,
    cast(r.imp_utilidad as decimal(30, 2)) as fc_adm_resultado_neto,
    cast(r.imp_patrimonio as decimal(30, 2)) as fc_adm_patrimonio_neto,
    r.fec_balance_ddjj as dt_adm_fec_balance_ddjj,
    r.pefecalt as dt_adm_pefecalt,
    r.pefecmod as dt_adm_pefecmod,
    RANK() OVER(
        PARTITION BY r.penumper ORDER BY r.anio_ganancias DESC,
        r.pefecalt DESC)
    as int_adm_orden,
    date_format(r.partition_date,'yyyyMM') as ds_adm_periodo
from bi_corp_staging.sge_resumen_crediticio r
where r.anio_ganancias is not null and r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';