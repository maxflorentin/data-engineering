set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;



CREATE TEMPORARY TABLE bi_corp_bdr.clientes_en_mora as
select distinct num_persona as num_persona, fec_alta_submarca_cliente as max_fec_inicio_vigencia,
case when substring(cm.partition_date, 1, 7) <= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
then cm.partition_date else cast(null as string)
end as max_fec_baja,
'mora' as source
from bi_corp_staging.view_clientes_en_mora cm
where cm.cod_submarca_cliente IN ('20', '22') and cm.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
;

CREATE TEMPORARY TABLE bi_corp_bdr.marca_comite as
select distinct mc.num_persona, mc.fec_inicio_vigencia, case when mc_old.fec_inicio_vigencia is null then 'y' else 'n' end as inicio_vigencia, mc.fec_revision as fec_baja, mc.data_date_part
from santander_business_risk_arda.marca_comite mc
left join santander_business_risk_arda.marca_comite mc_old on mc_old.num_persona = mc.num_persona and substring(mc_old.fec_inicio_vigencia, 1, 7) = substring(cast(add_months(mc.fec_inicio_vigencia ,-1) as string),  1, 7) and mc_old.cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO')
where mc.data_date_part <= '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_marca_comite', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
and mc.cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO')
;

CREATE TEMPORARY TABLE bi_corp_bdr.marca_comite_max_inicio as
select num_persona,
max(fec_inicio_vigencia) as max_fec_inicio_vigencia
from bi_corp_bdr.marca_comite
where inicio_vigencia = 'y' and substring(fec_inicio_vigencia, 6, 2) != '29'
group by num_persona
;

CREATE TEMPORARY TABLE bi_corp_bdr.marca_comite_max_baja as
select num_persona,
max(fec_baja) as max_fec_baja
from bi_corp_bdr.marca_comite
group by num_persona
;

CREATE TEMPORARY TABLE bi_corp_bdr.marca_comite_filter as
select i.num_persona,
max_fec_inicio_vigencia,
case when max_fec_baja >= max_fec_inicio_vigencia and substring(max_fec_baja, 1, 7) <= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
then max_fec_baja
else cast(null as string)
end as max_fec_baja,
'comite' as source
from bi_corp_bdr.marca_comite_max_inicio i
left join bi_corp_bdr.marca_comite_max_baja b on b.num_persona = i.num_persona
;

CREATE TEMPORARY TABLE bi_corp_bdr.utp_union as
select distinct num_persona, trim(max_fec_inicio_vigencia) as max_fec_inicio_vigencia, trim(max_fec_baja) as max_fec_baja, source
from bi_corp_bdr.marca_comite_filter
union all
select distinct num_persona, trim(max_fec_inicio_vigencia) as max_fec_inicio_vigencia, trim(max_fec_baja) as max_fec_baja, source
from bi_corp_bdr.clientes_en_mora
;


insert overwrite table bi_corp_bdr.marca_utp_clientes partition(partition_date)
select distinct u.num_persona,
case when m.num_persona is null then c.max_fec_inicio_vigencia
when c.num_persona is null then m.max_fec_inicio_vigencia
when c.max_fec_baja is null and m.max_fec_baja is null and c.max_fec_inicio_vigencia >= m.max_fec_inicio_vigencia then m.max_fec_inicio_vigencia
when c.max_fec_baja is null and m.max_fec_baja is null and c.max_fec_inicio_vigencia < m.max_fec_inicio_vigencia then c.max_fec_inicio_vigencia
when c.max_fec_baja is not null and c.max_fec_baja >= m.max_fec_inicio_vigencia and m.max_fec_inicio_vigencia >= c.max_fec_inicio_vigencia then c.max_fec_inicio_vigencia
when c.max_fec_baja is not null and c.max_fec_baja >= m.max_fec_inicio_vigencia and m.max_fec_inicio_vigencia < c.max_fec_inicio_vigencia then m.max_fec_inicio_vigencia
when c.max_fec_baja is not null and c.max_fec_baja < m.max_fec_inicio_vigencia then m.max_fec_inicio_vigencia
when m.max_fec_baja is not null and m.max_fec_baja >= c.max_fec_inicio_vigencia and c.max_fec_inicio_vigencia >= m.max_fec_inicio_vigencia then m.max_fec_inicio_vigencia
when m.max_fec_baja is not null and m.max_fec_baja >= c.max_fec_inicio_vigencia and c.max_fec_inicio_vigencia < m.max_fec_inicio_vigencia then c.max_fec_inicio_vigencia
when m.max_fec_baja is not null and m.max_fec_baja <  c.max_fec_inicio_vigencia then c.max_fec_inicio_vigencia
end as max_fec_inicio_vigencia,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as partition_date -- MONTH PARTITION BDR
from bi_corp_bdr.utp_union u
left join bi_corp_bdr.utp_union c on c.num_persona = u.num_persona and c.source = 'comite' and c.max_fec_inicio_vigencia != '' and c.max_fec_inicio_vigencia is not null
left join bi_corp_bdr.utp_union m on m.num_persona = u.num_persona and m.source = 'mora' and m.max_fec_inicio_vigencia != '' and m.max_fec_inicio_vigencia is not null
where u.max_fec_inicio_vigencia != '' and u.max_fec_inicio_vigencia is not null;