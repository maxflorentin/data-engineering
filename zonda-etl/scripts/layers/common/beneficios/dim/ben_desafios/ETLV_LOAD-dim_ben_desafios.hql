set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.bt_ben_desafios
partition(partition_date)

Select
c.id as cod_ben_desafio,
c.name as ds_ben_nombredesafio,
c.description as ds_ben_descripciondesafio,
c.pointsduration as fc_ben_puntosduracion,
substring(c.datefrom,1,10) as dt_ben_desde,
substring(c.dateto,1,10) as dt_ben_hasta,
c.hasautomaticrenewal as cod_ben_renovacion,
case when substring(c.originaldatefrom,1,10) = 'null' then null else substring(c.originaldatefrom,1,10) end as dt_ben_desdeoriginal,
case when substring(c.originaldateto,1,10) = 'null' then null else substring(c.originaldateto,1,10) end as dt_hastaoriginal,
c.triggerpoints as fc_ben_puntosencadenado,
c.id_trigger as cod_ben_encadenado,
c.id_bonuscode as cod_ben_codigobonus,
case when c.id_challengesfather = 'null' then null else c.id_challengesfather end as cod_ben_desafiopadre,
c.id_challengestate as cod_ben_estadodesafio,
c.only_one_time as cod_ben_tiempo,
cl.id_level as cod_ben_nivel,
c.partition_date

from
bi_corp_staging.rio265_challenge c
left join bi_corp_staging.rio265_challengelevel cl
on cl.id_challenge = c.id
and c.partition_date = cl.partition_date
Where
c.id_challengestate IN (2, 7, 9)
and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtcoo', dag_id='LOAD_CMN_Beneficios') }}'; --'2021-01-06'
