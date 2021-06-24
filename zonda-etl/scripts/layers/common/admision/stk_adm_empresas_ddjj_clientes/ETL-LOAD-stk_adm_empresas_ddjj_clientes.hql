set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_ddjj_clientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(ddjj.id_ddjj as int) as id_adm_ddjj,
    cast(ddjj.penumper as int) as cod_adm_numero_persona_sge,
    if(ddjj.fec_ddjj > TO_DATE('1753-01-01') AND ddjj.fec_ddjj < TO_DATE('2099-12-31'), ddjj.fec_ddjj, null) as dt_adm_fecha,
    date_format(if(ddjj.fec_ddjj > TO_DATE('1753-01-01') AND ddjj.fec_ddjj < TO_DATE('2099-12-31'), ddjj.fec_ddjj, null),'yyyyMM') as ds_adm_periodo_ddjj,
    cast(ddjj.res_neto_djig as decimal(15,2)) as fc_adm_resultado_neto,
    cast(ddjj.amort_djig as decimal(15,2)) as fc_adm_amortizaciones,
    cast(ddjj.vtas_anuales as decimal(15,2)) as fc_adm_ventas_anuales,
    cast(ddjj.pat_neto_total as decimal(15,2)) as fc_adm_patrimonio_neto_total,
    cast(ddjj.deu_bancaria as decimal(15,2)) as fc_adm_deuda_bancaria,
    RANK() OVER(
        PARTITION BY ddjj.penumper
        ORDER BY ddjj.fec_ddjj DESC, ddjj.fec_ddjj DESC
    ) AS int_adm_orden
from bi_corp_staging.sge_ddjj_per ddjj
where ddjj.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';