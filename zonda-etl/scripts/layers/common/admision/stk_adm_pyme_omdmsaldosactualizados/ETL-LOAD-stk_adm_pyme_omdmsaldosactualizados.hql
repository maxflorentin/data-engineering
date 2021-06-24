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


DROP TABLE IF EXISTS saldosactualizados;
CREATE TEMPORARY TABLE saldosactualizados AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes_propuesta_pyme omdm
lateral view json_tuple(json,
'accTarjeta',
'banco',
'chequesCesionados',
'codBanco',
'plazoRestante',
'saldoDeuda'
) b as accTarjeta, banco, chequesCesionados, codBanco, plazoRestante, saldoDeuda
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'SaldosActualizados';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_omdmsaldosactualizados
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select distinct
omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
cast(omdm.accTarjeta as double) as fc_adm_acctarjeta,
omdm.banco as ds_adm_banco,
cast(omdm.chequesCesionados as double) as fc_adm_chequescesionados,
cast(omdm.codBanco AS int) as cod_adm_codbanco,
cast(omdm.plazoRestante as int) as int_adm_plazorestante,
cast(omdm.saldoDeuda as double) as fc_adm_saldodeuda,
omdm.json as ds_adm_json
from saldosactualizados omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;