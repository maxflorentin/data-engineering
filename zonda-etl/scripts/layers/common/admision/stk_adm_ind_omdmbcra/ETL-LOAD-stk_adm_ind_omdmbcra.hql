set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS bcra;
CREATE TEMPORARY TABLE bcra AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'entidadBCRA',
'montoBCRA',
'periodoBCRA',
'situacionBCRA'
) b as entidadBCRA,montoBCRA,periodoBCRA,situacionBCRA
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'BCRA';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmbcra
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.entidadBCRA as cod_adm_entidadbcra,
cast(omdm.montoBCRA as double) as fc_adm_montobcra,
cast(omdm.periodoBCRA as int) as int_adm_periodobcra,
omdm.situacionBCRA as cod_adm_situacionbcra,
omdm.json as ds_adm_json
from bcra omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
