set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with contratos_nuevos as (
select g.num_persona, g.cod_entidad as new_entidad, g.cod_centro as new_centro, g.num_cuenta as new_cuenta, g.cod_producto as new_producto, g.cod_subprodu as new_subprodu, g.cod_divisa as divisa, g.data_date_part as fec_renum
from santander_business_risk_arda.contratos_garra g
where g.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='LOAD_BDR_UPGRADE_TARJETAS') }}'
and g.cod_producto in ('40', '41', '42')
group by g.num_persona, g.cod_entidad, g.cod_centro, g.num_cuenta, g.cod_producto, g.cod_subprodu, g.data_date_part, g.cod_divisa
),
contratos_viejos as (
select g.num_persona as nup, g.cod_entidad as old_entidad, g.cod_centro as old_centro, g.num_cuenta as old_cuenta, g.cod_producto as old_producto, g.cod_subprodu as old_subprodu, g.data_date_part, sum(g.imp_riesgo_divisa_local_del_contrato) as imp_reest
from santander_business_risk_arda.contratos_garra g
left join santander_business_risk_arda.calendario c on c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='LOAD_BDR_UPGRADE_TARJETAS') }}'
and c.data_date_part = c.fec_yyyymmdd
where  g.data_date_part = c.laborable_anterior_yyyymmdd
and g.cod_producto in ('40', '41', '42')
group by g.num_persona, g.cod_entidad, g.cod_centro, g.num_cuenta, g.cod_producto, g.cod_subprodu, g.data_date_part
)
INSERT OVERWRITE TABLE bi_corp_bdr.upgrade_tarjetas
PARTITION(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR_UPGRADE_TARJETAS') }}')
SELECT DISTINCT
cv.nup,
cv.old_entidad,
cv.old_centro,
cv.old_cuenta,
cv.old_producto,
cv.old_subprodu,
cn.new_entidad,
cn.new_centro,
cn.new_cuenta,
cn.new_producto,
cn.new_subprodu,
cv.imp_reest,
cn.divisa,
to_date(from_unixtime(UNIX_TIMESTAMP(cast(cn.fec_renum as varchar(10)),"yyyyMMdd"))) as fec_renum
FROM contratos_viejos cv, contratos_nuevos cn
WHERE cv.nup = cn.num_persona and
cv.old_entidad = cn.new_entidad and
cv.old_centro = cn.new_centro and
substring(cv.old_cuenta, 2) = substring(cn.new_cuenta, 2) and
cv.old_producto = cn.new_producto and
cv.old_subprodu != cn.new_subprodu;