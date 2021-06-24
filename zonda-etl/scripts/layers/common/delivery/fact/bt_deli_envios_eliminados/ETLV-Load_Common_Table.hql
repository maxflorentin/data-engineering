set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_deli_envios_eliminados

select
        a.crupier_id as  cod_deli_crupier
        ,a.fecha as  ts_deli_eliminado
		,min(a.partition_date)

from  bi_corp_staging.rio258_cp_envios_eliminados a

group by a.crupier_id, a.fecha
