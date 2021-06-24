set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes_operacion_pyme
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS operacionnosis;
CREATE TEMPORARY TABLE operacionnosis AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
label,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes_operacion_pyme omdm
lateral view json_tuple(json,
'nosisVariable',
'nosisVariableAC',
'nosisVariableDF',
'nosisVariableGO',
'nosisVariablePS',
'nosisVariableVV'
) b as nosisVariable, nosisVariableAC, nosisVariableDF, nosisVariableGO, nosisVariablePS, nosisVariableVV
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label like 'Nosis%';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_omdmoperacionnosis
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select distinct
omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.label as ds_adm_variable,
omdm.nosisVariable as cod_adm_nosisvariable,
omdm.nosisVariableAC as cod_adm_nosisvariableac,
omdm.nosisVariableDF as cod_adm_nosisvariabledf,
omdm.nosisVariableGO as cod_adm_nosisvariablego,
omdm.nosisVariablePS as cod_adm_nosisvariableps,
omdm.nosisVariableVV as cod_adm_nosisvariablevv,
omdm.json as ds_adm_json
from operacionnosis omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
