insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_flujos')
--No deben existir registros duplicados (3.14.1) R15753141
select
    jmf.r9746_feoperac as fecha_proceso,
    'R15753141' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct concat(jmf.r9746_contra1, jmf.r9746_fecmvto, jmf.partition_date)) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_flujos jmf
where jmf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jmf.r9746_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_flujos')
-- El valor del campo Empresa no existe (3.14.1) R15763141
select
    jmf.r9746_feoperac as fecha_proceso,
    'R15763141' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when cast(r9746_s1emp as int) = 0 then 1 else 0 end) as num_reg_error
,from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_flujos jmf
where jmf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jmf.r9746_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_flujos')
-- El identificador del contrato debe estar informado y no ser cero (3.14.1) R15773141
select
    jmf.r9746_feoperac as fecha_proceso,
    'R15773141' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when cast(R9746_CONTRA1 as int) = 000000000 then 1 else 0 end) as num_reg_error
,from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_flujos jmf
where jmf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jmf.r9746_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_flujos')
--El valor del campo clasificación del movimiento no existe (3.14.1) R15783141
select 
    jmf.r9746_feoperac as fecha_proceso,
    'R15783141' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when cast(r9746_clasmvto as int) = 00008 then 1 else 0 end) as num_reg_error
   ,from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_flujos jmf
where jmf.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jmf.r9746_feoperac;