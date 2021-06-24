set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS afip;
CREATE TEMPORARY TABLE afip AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'actMonotributo',
'baseAFIPMono',
'empleador',
'impGanancia',
'impIVA',
'integranteSoc',
'monotributo'
) b as actMonotributo,baseAFIPMono,empleador,impGanancia,impIVA,integranteSoc,monotributo
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Afip';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmafip
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.actMonotributo as cod_adm_actmonotributo,
omdm.baseAFIPMono as flag_adm_baseafipmono,
omdm.empleador as cod_adm_empleador,
omdm.impGanancia as cod_adm_impganancia,
omdm.impIVA as cod_adm_impiva,
omdm.integranteSoc as cod_adm_integrantesoc,
omdm.monotributo as cod_adm_monotributo,
omdm.json as ds_adm_json
from afip omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
