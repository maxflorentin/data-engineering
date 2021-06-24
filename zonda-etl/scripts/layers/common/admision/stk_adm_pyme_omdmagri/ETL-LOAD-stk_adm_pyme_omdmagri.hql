set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes_propuesta_pyme
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS agri;
CREATE TEMPORARY TABLE agri AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
label,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes_propuesta_pyme omdm
lateral view json_tuple(json,
'agroGastosEstructura',
'superficieAlquiladaAgriculturaMasFrutales',
'superficieAlquiladaCriaInvernadaTambo',
'superficieAparceriaAgriculturaMasFrutales',
'superficieAparceriaCriaInvernadaTambo',
'superficiePropiaAgriculturaMasFrutales',
'superficiePropiaCriaInvernadaTambo',
'agriCostoComercCultivoBSR',
'agriCostoDirectoCultivo',
'agriCostoDirectoCultivoBSR',
'agriHectareasCultivadas',
'agriHectareasCultivadasAlquiladas',
'agriHectareasCultivadasAparceria',
'agriHectareasCultivadasPropias',
'agriPrecioCultivoTN',
'agriPrecioCultivoTNBSR',
'agriRindeCultivo',
'agriRindeCultivoBSR',
'agriStockAlInicio',
'agriTipoCultivo',
'descCultivo',
'idCultivo',
'pctAparceria',
'zonaCultivo',
'agriZona'
) b as agroGastosEstructura, superficieAlquiladaAgriculturaMasFrutales, superficieAlquiladaCriaInvernadaTambo, superficieAparceriaAgriculturaMasFrutales, superficieAparceriaCriaInvernadaTambo, superficiePropiaAgriculturaMasFrutales, superficiePropiaCriaInvernadaTambo, agriCostoComercCultivoBSR, agriCostoDirectoCultivo, agriCostoDirectoCultivoBSR, agriHectareasCultivadas, agriHectareasCultivadasAlquiladas, agriHectareasCultivadasAparceria, agriHectareasCultivadasPropias, agriPrecioCultivoTN, agriPrecioCultivoTNBSR, agriRindeCultivo, agriRindeCultivoBSR, agriStockAlInicio, agriTipoCultivo, descCultivo, idCultivo, pctAparceria, zonaCultivo, agriZona
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Agri';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_omdmagri
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select distinct
omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.label as ds_adm_variable,
cast(omdm.agroGastosEstructura as double) as fc_adm_agrogastosestructura,
cast(omdm.superficieAlquiladaAgriculturaMasFrutales as int) as int_adm_superficiealquiladaagriculturamasfrutales,
cast(omdm.superficieAlquiladaCriaInvernadaTambo as int) as int_adm_superficiealquiladacriainvernadatambo,
cast(omdm.superficieAparceriaAgriculturaMasFrutales as int) as int_adm_superficieaparceriaagriculturamasfrutales,
cast(omdm.superficieAparceriaCriaInvernadaTambo as int) as int_adm_superficieaparceriacriainvernadatambo,
cast(omdm.superficiePropiaAgriculturaMasFrutales as int) as int_adm_superficiepropiaagriculturamasfrutales,
cast(omdm.superficiePropiaCriaInvernadaTambo as int) as int_adm_superficiepropiacriainvernadatambo,
cast(omdm.agriCostoComercCultivoBSR as double) as fc_adm_agricostocomerccultivobsr,
cast(omdm.agriCostoDirectoCultivo as double) as fc_adm_agricostodirectocultivo,
cast(omdm.agriCostoDirectoCultivoBSR as double) as fc_adm_agricostodirectocultivobsr,
cast(omdm.agriHectareasCultivadas as double) as fc_adm_agrihectareascultivadas,
cast(omdm.agriHectareasCultivadasAlquiladas as double) as fc_adm_agrihectareascultivadasalquiladas,
cast(omdm.agriHectareasCultivadasAparceria as double) as fc_adm_agrihectareascultivadasaparceria,
cast(omdm.agriHectareasCultivadasPropias as double) as fc_adm_agrihectareascultivadaspropias,
cast(omdm.agriPrecioCultivoTN as double) as fc_adm_agripreciocultivotn,
cast(omdm.agriPrecioCultivoTNBSR as double) as fc_adm_agripreciocultivotnbsr,
cast(omdm.agriRindeCultivo as double) as fc_adm_agririndecultivo,
cast(omdm.agriRindeCultivoBSR as double) as fc_adm_agririndecultivoBSR,
cast(omdm.agriStockAlInicio as double) as fc_adm_agristockalinicio,
cast(omdm.agriTipoCultivo as int) as cod_adm_agritipocultivo,
omdm.descCultivo as ds_adm_desccultivo,
cast(omdm.idCultivo as int) as cod_adm_idcultivo,
cast(omdm.pctAparceria as double) as fc_adm_pctaparceria,
omdm.zonaCultivo as ds_adm_zonacultivo,
omdm.agriZona as ds_adm_agrizona,
omdm.json as ds_adm_json
from agri omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;


