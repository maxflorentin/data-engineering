set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS bgdtrpp_stock_tmp;
create temporary table bgdtrpp_stock_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtrpp where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates') }}'
union all
SELECT * FROM bi_corp_staging.malbgc_bgdtrpp_stock where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates') }}';

DROP TABLE IF EXISTS bgdtrpp_update_tmp;
create TEMPORARY table bgdtrpp_update_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtrpp_updates where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}';

INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtrpp PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.entidad,a.entidad) as entidad,
COALESCE(b.centro_alta,a.centro_alta) as centro_alta,
COALESCE(b.paquete,a.paquete) as paquete,
COALESCE(b.entidad2,a.entidad2) as entidad2,
COALESCE(b.centro_alta2,a.centro_alta2) as centro_alta2,
COALESCE(b.cuenta,a.cuenta) as cuenta,
COALESCE(b.producto,a.producto) as producto,
COALESCE(b.subprodu,a.subprodu) as subprodu,
COALESCE(b.divisa1,a.divisa1) as divisa1,
COALESCE(b.divisa2,a.divisa2) as divisa2,
COALESCE(b.ind_cta_piv,a.ind_cta_piv) as ind_cta_piv,
COALESCE(b.estarel,a.estarel) as estarel,
COALESCE(b.entidad_umo,a.entidad_umo) as entidad_umo,
COALESCE(b.centro_umo,a.centro_umo) as centro_umo,
COALESCE(b.userid_umo,a.userid_umo) as userid_umo,
COALESCE(b.netname_umo,a.netname_umo) as netname_umo,
COALESCE(b.timest_umo,a.timest_umo) as timest_umo
from bgdtrpp_stock_tmp a
full outer join bgdtrpp_update_tmp b on ((a.entidad = b.entidad) AND (a.centro_alta = b.centro_alta) AND (a.paquete = b.paquete) AND (a.entidad2 = b.entidad2) AND (a.centro_alta2 = b.centro_alta2) AND (a.cuenta = b.cuenta));