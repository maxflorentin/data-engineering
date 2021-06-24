set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select
    tipo_tramite,
    cod_tramite,
    b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes_propuesta_empresas
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}' and label = 'Participante'
group by tipo_tramite, cod_tramite, b.tipoParticipante;


DROP TABLE IF EXISTS invernada;
CREATE TEMPORARY TABLE invernada AS
select
    tipo_tramite,
    cod_tramite,
    fec_proceso as fec_proceso,
    label,
    b.*,
    json
from bi_corp_staging.scoring_omdm_participantes_propuesta_empresas omdm
lateral view json_tuple(json,
    'totalCompras',
    'totalCostoDirecto',
    'totalFacturacionBruta',
    'totalGastosComercializacion'
) b as
    totalCompras,
    totalCostoDirecto,
    totalFacturacionBruta,
    totalGastosComercializacion
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}' and label like 'Invernada%';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_omdm_cpp_invernada
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}')
select
    omdm.tipo_tramite as cod_adm_tipotramite,
    omdm.cod_tramite as cod_adm_tramite,
    substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
    cast(omdm.totalCompras as double) fc_Adm_totalCompras,
    cast(omdm.totalCostoDirecto as double) fc_adm_totalCostoDirecto,
    cast(omdm.totalFacturacionBruta as double) fc_adm_totalFacturacionBruta,
    cast(omdm.totalGastosComercializacion as double) fc_adm_totalGastosComercializacion,
    omdm.json as ds_adm_json
from invernada omdm
left join participantes p on p.tipo_tramite = omdm.tipo_tramite and p.cod_tramite = omdm.cod_tramite;