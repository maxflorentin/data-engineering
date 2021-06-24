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
    participante,
    b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes_propuesta_empresas
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}' and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS cria;
CREATE TEMPORARY TABLE cria AS
select
    tipo_tramite,
    cod_tramite,
    participante,
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
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}' and label like 'Cria%';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_omdm_cpp_cria
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_OMDM_Empresas') }}')
select
    omdm.tipo_tramite as cod_adm_tipotramite,
    omdm.cod_tramite as cod_adm_tramite,
    substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
    p.tipo_participante as cod_adm_tipoparticipante,
    cast(omdm.totalCompras as double) as fc_adm_totalcompras,
    cast(omdm.totalCostoDirecto as double) as fc_adm_totalcostodirecto,
    cast(omdm.totalFacturacionBruta as double) as fc_adm_totalfacturacionbruta,
    cast(omdm.totalGastosComercializacion as double) as fc_adm_totalgastoscomercializacion,
    omdm.json as ds_adm_json
from cria omdm
left join participantes p on p.tipo_tramite = omdm.tipo_tramite and p.cod_tramite = omdm.cod_tramite and p.participante = omdm.participante;