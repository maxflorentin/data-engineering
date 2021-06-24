set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS max_stk_log_cuotaphone;
CREATE TEMPORARY TABLE max_stk_log_cuotaphone AS
SELECT max(cf.partition_date) AS partition_date
FROM bi_corp_staging.garra_stk_log_cuotaphone cf
WHERE cf.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}',7)
and cf.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}';

DROP TABLE IF EXISTS log_cuotaphone_update_tmp_fix_duplicados;
create TEMPORARY table log_cuotaphone_update_tmp_fix_duplicados as
select lc.gitccuo_num_persona,
lc.gitccuo_cod_centro,
lc.gitccuo_num_contrato,
lc.gitccuo_cod_divisa,
lc.gitccuo_cod_producto,
lc.gitccuo_cod_subprodu,
lc.partition_date,
lc.gitccuo_num_secuencia,
max(lc.gitccuo_fec_cuotaphone) as gitccuo_fec_cuotaphone
from bi_corp_staging.garra_log_cuotaphone lc
where lc.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}'
group by lc.gitccuo_num_persona,
lc.gitccuo_cod_centro,
lc.gitccuo_num_contrato,
lc.gitccuo_cod_divisa,
lc.gitccuo_cod_producto,
lc.gitccuo_cod_subprodu,
lc.partition_date,
lc.gitccuo_num_secuencia;

DROP TABLE IF EXISTS log_cuotaphone_stock_tmp;
create temporary table log_cuotaphone_stock_tmp as
SELECT cr.gitccuo_cod_entigen,
cr.gitccuo_num_persona,
cr.gitccuo_cod_entidad,
cr.gitccuo_cod_centro,
cr.gitccuo_num_contrato,
cr.gitccuo_cod_producto,
cr.gitccuo_cod_subprodu,
cr.gitccuo_cod_divisa,
cr.gitccuo_num_secuencia,
cr.gitccuo_fec_cuotaphone,
cr.gitccuo_cuenta_visa,
cr.gitccuo_cod_marca_ini,
cr.gitccuo_cod_submarca_ini,
cr.gitccuo_cod_marca_seg_ini,
cr.gitccuo_tip_reestruct_ini,
cr.gitccuo_cod_marca_act,
cr.gitccuo_cod_submarca_act,
cr.gitccuo_fec_cambio_seg,
cr.gitccuo_cod_marca_seg_act,
cr.gitccuo_tip_reestruct_act,
cr.gitccuo_fec_cura,
cr.gitccuo_fec_empeora,
cr.gitccuo_fec_normaliza,
cr.gitccuo_cod_criterio,
cr.gitccuo_motivo_cambio,
cr.gitccuo_num_ree,
cr.gitccuo_entidad_umo,
cr.gitccuo_centro_umo,
cr.gitccuo_userid_umo,
cr.gitccuo_netname_umo,
cr.gitccuo_timest_umo,
cr.partition_date
FROM bi_corp_staging.garra_stk_log_cuotaphone cr
INNER JOIN max_stk_log_cuotaphone mcr ON mcr.partition_date = cr.partition_date
WHERE cr.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}',7)
and cr.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}'

union all

SELECT distinct s.gitccuo_cod_entigen,
s.gitccuo_num_persona,
s.gitccuo_cod_entidad,
s.gitccuo_cod_centro,
s.gitccuo_num_contrato,
s.gitccuo_cod_producto,
s.gitccuo_cod_subprodu,
s.gitccuo_cod_divisa,
s.gitccuo_num_secuencia,
s.gitccuo_fec_cuotaphone,
s.gitccuo_cuenta_visa,
s.gitccuo_cod_marca_ini,
s.gitccuo_cod_submarca_ini,
s.gitccuo_cod_marca_seg_ini,
s.gitccuo_tip_reestruct_ini,
s.gitccuo_cod_marca_act,
s.gitccuo_cod_submarca_act,
s.gitccuo_fec_cambio_seg,
s.gitccuo_cod_marca_seg_act,
s.gitccuo_tip_reestruct_act,
s.gitccuo_fec_cura,
s.gitccuo_fec_empeora,
s.gitccuo_fec_normaliza,
s.gitccuo_cod_criterio,
s.gitccuo_motivo_cambio,
s.gitccuo_num_ree,
s.gitccuo_entidad_umo,
s.gitccuo_centro_umo,
s.gitccuo_userid_umo,
s.gitccuo_netname_umo,
s.gitccuo_timest_umo,
s.partition_date
FROM bi_corp_staging.garra_log_cuotaphone s
inner join log_cuotaphone_update_tmp_fix_duplicados f on
f.gitccuo_num_persona=s.gitccuo_num_persona
and f.gitccuo_cod_centro=s.gitccuo_cod_centro
and f.gitccuo_num_contrato=s.gitccuo_num_contrato
and f.gitccuo_cod_divisa=s.gitccuo_cod_divisa
and f.gitccuo_cod_producto=s.gitccuo_cod_producto
and f.gitccuo_cod_subprodu=s.gitccuo_cod_subprodu
and f.gitccuo_num_secuencia=s.gitccuo_num_secuencia
and f.gitccuo_fec_cuotaphone=s.gitccuo_fec_cuotaphone
and f.partition_date=s.partition_date
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}'='2020-11-03' AND s.partition_date='2020-11-03';


DROP TABLE IF EXISTS log_cuotaphone_update_tmp;
create TEMPORARY table log_cuotaphone_update_tmp as
SELECT distinct
g.gitccuo_cod_entigen,
g.gitccuo_num_persona,
g.gitccuo_cod_entidad,
g.gitccuo_cod_centro,
g.gitccuo_num_contrato,
g.gitccuo_cod_producto,
g.gitccuo_cod_subprodu,
g.gitccuo_cod_divisa,
g.gitccuo_num_secuencia,
g.gitccuo_fec_cuotaphone,
g.gitccuo_cuenta_visa,
g.gitccuo_cod_marca_ini,
g.gitccuo_cod_submarca_ini,
g.gitccuo_cod_marca_seg_ini,
g.gitccuo_tip_reestruct_ini,
g.gitccuo_cod_marca_act,
g.gitccuo_cod_submarca_act,
g.gitccuo_fec_cambio_seg,
g.gitccuo_cod_marca_seg_act,
g.gitccuo_tip_reestruct_act,
g.gitccuo_fec_cura,
g.gitccuo_fec_empeora,
g.gitccuo_fec_normaliza,
g.gitccuo_cod_criterio,
g.gitccuo_motivo_cambio,
g.gitccuo_num_ree,
g.gitccuo_entidad_umo,
g.gitccuo_centro_umo,
g.gitccuo_userid_umo,
g.gitccuo_netname_umo,
g.gitccuo_timest_umo,
g.partition_date
FROM bi_corp_staging.garra_log_cuotaphone g
inner join log_cuotaphone_update_tmp_fix_duplicados f on f.gitccuo_num_persona=g.gitccuo_num_persona
and f.gitccuo_cod_centro=g.gitccuo_cod_centro
and f.gitccuo_num_contrato=g.gitccuo_num_contrato
and f.gitccuo_cod_divisa=g.gitccuo_cod_divisa
and f.gitccuo_cod_producto=g.gitccuo_cod_producto
and f.gitccuo_cod_subprodu=g.gitccuo_cod_subprodu
and f.gitccuo_num_secuencia=g.gitccuo_num_secuencia
and f.gitccuo_fec_cuotaphone=g.gitccuo_fec_cuotaphone
and f.partition_date=g.partition_date
where g.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_staging.garra_stk_log_cuotaphone
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_GARRA_Updates_Log_Cuotaphone-Daily') }}')
SELECT
COALESCE(b.gitccuo_cod_entigen,a.gitccuo_cod_entigen) as gitccuo_cod_entigen,
COALESCE(b.gitccuo_num_persona,a.gitccuo_num_persona) as gitccuo_num_persona,
COALESCE(b.gitccuo_cod_entidad,a.gitccuo_cod_entidad) as gitccuo_cod_entidad,
COALESCE(b.gitccuo_cod_centro,a.gitccuo_cod_centro) as gitccuo_cod_centro,
COALESCE(b.gitccuo_num_contrato,a.gitccuo_num_contrato) as gitccuo_num_contrato,
COALESCE(b.gitccuo_cod_producto,a.gitccuo_cod_producto) as gitccuo_cod_producto,
COALESCE(b.gitccuo_cod_subprodu,a.gitccuo_cod_subprodu) as gitccuo_cod_subprodu,
COALESCE(b.gitccuo_cod_divisa,a.gitccuo_cod_divisa) as gitccuo_cod_divisa,
COALESCE(b.gitccuo_num_secuencia,a.gitccuo_num_secuencia) as gitccuo_num_secuencia,
COALESCE(b.gitccuo_fec_cuotaphone,a.gitccuo_fec_cuotaphone) as gitccuo_fec_cuotaphone,
COALESCE(b.gitccuo_cuenta_visa,a.gitccuo_cuenta_visa) as gitccuo_cuenta_visa,
COALESCE(b.gitccuo_cod_marca_ini,a.gitccuo_cod_marca_ini) as gitccuo_cod_marca_ini,
COALESCE(b.gitccuo_cod_submarca_ini,a.gitccuo_cod_submarca_ini) as gitccuo_cod_submarca_ini,
COALESCE(b.gitccuo_cod_marca_seg_ini,a.gitccuo_cod_marca_seg_ini) as gitccuo_cod_marca_seg_ini,
COALESCE(b.gitccuo_tip_reestruct_ini,a.gitccuo_tip_reestruct_ini) as gitccuo_tip_reestruct_ini,
COALESCE(b.gitccuo_cod_marca_act,a.gitccuo_cod_marca_act) as gitccuo_cod_marca_act,
COALESCE(b.gitccuo_cod_submarca_act,a.gitccuo_cod_submarca_act) as gitccuo_cod_submarca_act,
COALESCE(b.gitccuo_fec_cambio_seg,a.gitccuo_fec_cambio_seg) as gitccuo_fec_cambio_seg,
COALESCE(b.gitccuo_cod_marca_seg_act,a.gitccuo_cod_marca_seg_act) as gitccuo_cod_marca_seg_act,
COALESCE(b.gitccuo_tip_reestruct_act,a.gitccuo_tip_reestruct_act) as gitccuo_tip_reestruct_act,
COALESCE(b.gitccuo_fec_cura,a.gitccuo_fec_cura) as gitccuo_fec_cura,
COALESCE(b.gitccuo_fec_empeora,a.gitccuo_fec_empeora) as gitccuo_fec_empeora,
COALESCE(b.gitccuo_fec_normaliza,a.gitccuo_fec_normaliza) as gitccuo_fec_normaliza,
COALESCE(b.gitccuo_cod_criterio,a.gitccuo_cod_criterio) as gitccuo_cod_criterio,
COALESCE(b.gitccuo_motivo_cambio,a.gitccuo_motivo_cambio) as gitccuo_motivo_cambio,
COALESCE(b.gitccuo_num_ree,a.gitccuo_num_ree) as gitccuo_num_ree,
COALESCE(b.gitccuo_entidad_umo,a.gitccuo_entidad_umo) as gitccuo_entidad_umo,
COALESCE(b.gitccuo_centro_umo,a.gitccuo_centro_umo) as gitccuo_centro_umo,
COALESCE(b.gitccuo_userid_umo,a.gitccuo_userid_umo) as gitccuo_userid_umo,
COALESCE(b.gitccuo_netname_umo,a.gitccuo_netname_umo) as gitccuo_netname_umo,
COALESCE(b.gitccuo_timest_umo,a.gitccuo_timest_umo) as gitccuo_timest_umo
from log_cuotaphone_stock_tmp a
full outer join log_cuotaphone_update_tmp b on
cast(a.gitccuo_num_persona as int)=cast(b.gitccuo_num_persona as int)
and cast(a.gitccuo_cod_centro as int)=cast(b.gitccuo_cod_centro as int)
and cast(a.gitccuo_num_contrato as bigint)=cast(b.gitccuo_num_contrato as bigint)
and cast(a.gitccuo_cod_producto as int)=cast(b.gitccuo_cod_producto as int)
and trim(a.gitccuo_cod_subprodu)=trim(b.gitccuo_cod_subprodu)
and trim(a.gitccuo_cod_divisa)=trim(b.gitccuo_cod_divisa)
and cast(a.gitccuo_num_secuencia as int)=cast(b.gitccuo_num_secuencia as int);