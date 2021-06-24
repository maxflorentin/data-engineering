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


DROP TABLE IF EXISTS datos_adicionales;
CREATE TEMPORARY TABLE datos_adicionales AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'autoMarca',
'autoModelo',
'antMesVehiculo',
'cantidadHijosEdadEscolar',
'cantidadMesesAltaMatriculaProfesional',
'marcaGrupoMedicinaPrepaga',
'marcaTNO',
'matriculaAnio',
'montoMesCuotaColegio',
'montoMesExpensas',
'montoMesMedicinaPrepaga',
'profesion',
'valorVehiculo'
) b as autoMarca,autoModelo,antMesVehiculo,cantidadHijosEdadEscolar,cantidadMesesAltaMatriculaProfesional,marcaGrupoMedicinaPrepaga,marcaTNO,matriculaAnio,montoMesCuotaColegio,montoMesExpensas,montoMesMedicinaPrepaga,profesion,valorVehiculo
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'DatosAdicionales';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmdatosadicionales
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.autoMarca as ds_adm_automarca,
omdm.autoModelo as ds_adm_automodelo,
cast(omdm.antMesVehiculo as int) as int_adm_antmesvehiculo,
cast(omdm.cantidadHijosEdadEscolar as int) as int_adm_cantidadhijosedadescolar,
cast(omdm.cantidadMesesAltaMatriculaProfesional as int) as int_adm_cantidadmesesaltamatriculaprofesional,
omdm.marcaGrupoMedicinaPrepaga as cod_adm_marcagrupomedicinaprepaga,
omdm.marcaTNO as flag_adm_marcatno,
omdm.matriculaAnio as cod_adm_matriculaanio,
cast(omdm.montoMesCuotaColegio as double) as fc_adm_montomescuotacolegio,
cast(omdm.montoMesExpensas as double) as fc_adm_montomesexpensas,
cast(omdm.montoMesMedicinaPrepaga as double) as fc_adm_montomesmedicinaprepaga,
omdm.profesion as cod_adm_profesion,
cast(omdm.valorVehiculo as double) as fc_adm_valorvehiculo,
omdm.json as ds_adm_json
from datos_adicionales omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
