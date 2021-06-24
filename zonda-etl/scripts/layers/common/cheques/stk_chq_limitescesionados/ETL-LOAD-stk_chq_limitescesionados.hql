SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table bi_corp_common.stk_chq_limitescesionados
partition(partition_date)
select cast(c.ide_persona as bigint) as cod_per_nup,
substr(c.ide_paq,1,4) as cod_chq_entidad,
cast(substr(c.ide_paq,5,4) as bigint) as cod_suc_sucursal,
substr(c.ide_paq,9,12) as cod_chq_paquete,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),1,length(REGEXP_REPLACE(c.mon_cheq, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),-2))) as decimal(19, 2)),0) as fc_chq_limitecesion,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''),1,length(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''),-2))) as decimal(19, 2)),0) as fc_chq_montocesionado,
0 as fc_chq_montopendiente,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),1,length(REGEXP_REPLACE(c.mon_cheq, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),-2))) as decimal(19, 2)),0) - coalesce(cast(trim(concat(substr(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''),1,length(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(mc.imp_consumido, "^0+", ''),-2))) as decimal(19, 2)),0)  as fc_chq_montodisponible,
c.partition_date
from bi_corp_staging.acal_tcalif_paquete c
left join (select cod_nup, sum(imp_cheque) as imp_consumido
           from bi_corp_staging.malzx_alzxuchq
           where est_cheque in  ('GC', 'PE', 'EV', 'EC', 'FA')
           and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
           group by cod_nup) mc on mc.cod_nup=c.ide_persona
where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and coalesce(cast(trim(concat(substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),1,length(REGEXP_REPLACE(c.mon_cheq, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(c.mon_cheq, "^0+", ''),-2))) as decimal(19, 2)),0) > 0;