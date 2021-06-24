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


DROP TABLE IF EXISTS datos_universitarios;
CREATE TEMPORARY TABLE datos_universitarios AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'cantidadMateriasFaltantes',
'carrera',
'fechaEgreso',
'fechaIngreso',
'segmentoUniveritario',
'tipoUniversidad',
'universidad'
) b as cantidadMateriasFaltantes,carrera,fechaEgreso,fechaIngreso,segmentoUniveritario,tipoUniversidad,universidad
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'DatosUniversitarios';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmdatosuniversitarios
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
cast(omdm.cantidadMateriasFaltantes as int) as int_adm_cantidadmateriasfaltantes,
omdm.carrera as cod_adm_carrera,
omdm.fechaEgreso as dt_adm_fechaegreso,
omdm.fechaIngreso as dt_adm_fechaingreso,
omdm.segmentoUniveritario as cod_adm_segmentouniveritario,
omdm.tipoUniversidad as cod_adm_tipouniversidad,
omdm.universidad as cod_adm_universidad,
omdm.json as ds_adm_json
from datos_universitarios omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
