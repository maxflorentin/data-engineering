set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS bgdtmpa_stock_tmp;
create temporary table bgdtmpa_stock_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtmpa where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}'
union all
SELECT * FROM bi_corp_staging.malbgc_bgdtmpa_stock where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}';

DROP TABLE IF EXISTS bgdtmpa_update_tmp;
create TEMPORARY table bgdtmpa_update_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtmpa_updates where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}';

INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtmpa PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}')
SELECT
COALESCE(b.mpa_entidad,a.mpa_entidad) 				as mpa_entidad,
COALESCE(b.mpa_centro_alta,a.mpa_centro_alta) 		as mpa_centro_alta,
COALESCE(b.mpa_paquete,a.mpa_paquete) 				as mpa_paquete,
COALESCE(b.mpa_producto_paq,a.mpa_producto_paq) 	as mpa_producto_paq,
COALESCE(b.mpa_subprodu_paq,a.mpa_subprodu_paq) 	as mpa_subprodu_paq,
COALESCE(b.mpa_entidad_inv,a.mpa_entidad_inv) 		as mpa_entidad_inv,
COALESCE(b.mpa_centro_inv,a.mpa_centro_inv) 		as mpa_centro_inv,
COALESCE(b.mpa_cuenta_inv,a.mpa_cuenta_inv) 		as mpa_cuenta_inv,
COALESCE(b.mpa_entidad_contab,a.mpa_entidad_contab) as mpa_entidad_contab,
COALESCE(b.mpa_centro_contab,a.mpa_centro_contab) 	as mpa_centro_contab,
COALESCE(b.mpa_cod_plan,a.mpa_cod_plan) 			as mpa_cod_plan,
COALESCE(b.mpa_indesta,a.mpa_indesta) 				as mpa_indesta,
COALESCE(b.mpa_cod_sop_ext,a.mpa_cod_sop_ext) 		as mpa_cod_sop_ext,
COALESCE(b.mpa_pco_ecu_fhpr,a.mpa_pco_ecu_fhpr) 	as mpa_pco_ecu_fhpr,
COALESCE(b.mpa_fecha_alta,a.mpa_fecha_alta) 		as mpa_fecha_alta,
COALESCE(b.mpa_fecha_cancel,a.mpa_fecha_cancel) 	as mpa_fecha_cancel,
COALESCE(b.mpa_fecha_upgrade,a.mpa_fecha_upgrade) 	as mpa_fecha_upgrade,
COALESCE(b.mpa_fecha_downgr,a.mpa_fecha_downgr) 	as mpa_fecha_downgr,
COALESCE(b.mpa_ind_inhab_sbrg,a.mpa_ind_inhab_sbrg) as mpa_ind_inhab_sbrg,
COALESCE(b.mpa_cod_envio_mov,a.mpa_ind_inhab_sbrg) 	as mpa_ind_inhab_sbrg,
COALESCE(b.mpa_calpar_envio_mov,a.mpa_calpar_envio_mov) as mpa_calpar_envio_mov,
COALESCE(b.mpa_ordpar_envio_mov,a.mpa_ordpar_envio_mov) as mpa_ordpar_envio_mov,
COALESCE(b.mpa_entidad_umo,a.mpa_entidad_umo) 		as mpa_entidad_umo,
COALESCE(b.mpa_centro_umo,a.mpa_centro_umo) 		as mpa_centro_umo,
COALESCE(b.mpa_userid_umo,a.mpa_userid_umo) 		as mpa_userid_umo,
COALESCE(b.mpa_netname_umo,a.mpa_netname_umo) 		as mpa_netname_umo,
COALESCE(b.mpa_timest_umo,a.mpa_timest_umo) 		as mpa_timest_umo
from bgdtmpa_stock_tmp a
full outer join bgdtmpa_update_tmp b on (a.mpa_entidad=b.mpa_entidad) AND (a.mpa_centro_alta=b.mpa_centro_alta) AND (a.mpa_paquete=b.mpa_paquete);