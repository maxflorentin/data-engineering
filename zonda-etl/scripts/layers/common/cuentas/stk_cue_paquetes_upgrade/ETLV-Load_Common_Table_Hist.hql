set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- CALCULO POR CUENTA EL PAQUETE NOVEDAD QUE VIENE EN LA PARTICION
DROP TABLE IF EXISTS paquete_novedad;
create temporary table paquete_novedad as
select cod_suc_sucursal_paquete, cod_cue_cuenta, cod_cue_cuenta_paquete, cod_cue_producto_paquete, cod_cue_subproducto_paquete, dt_cue_upgrade, cod_cue_grupo_producto, cod_cue_estado_paquete
from
(
select
		bgdtrpp.centro_alta2 as cod_suc_sucursal_paquete,
		bgdtrpp.cuenta as cod_cue_cuenta,
		bgdtmpa.mpa_paquete as cod_cue_cuenta_paquete,
		bgdtmpa.mpa_producto_paq as cod_cue_producto_paquete,
		bgdtmpa.mpa_subprodu_paq as cod_cue_subproducto_paquete,
		bgdtrpp.timest_umo as ts_cue_actualizacion ,
		bgdtmpa.mpa_fecha_upgrade as dt_cue_upgrade,
		grupo_producto_id as cod_cue_grupo_producto,
		bgdtrpp.estarel as cod_cue_estado_paquete,
		row_number() over (PARTITION BY bgdtrpp.centro_alta2, bgdtrpp.cuenta order by estarel asc, timest_umo desc)  as ts_cue_actualizacion_calc
from  bi_corp_staging.malbgc_bgdtrpp bgdtrpp
LEFT JOIN bi_corp_staging.malbgc_bgdtmpa bgdtmpa
ON bgdtrpp.entidad = bgdtmpa.mpa_entidad
AND bgdtrpp.centro_alta = bgdtmpa.mpa_centro_alta
AND bgdtrpp.paquete = bgdtmpa.mpa_paquete
AND bgdtrpp.partition_date=bgdtmpa.partition_date
LEFT JOIN bi_corp_staging.exa_dim_productos exa_productos
on bgdtmpa.mpa_producto_paq=exa_productos.producto
and bgdtmpa.mpa_subprodu_paq=exa_productos.subproducto
and exa_productos.partition_date= case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2021-03-22' then '2021-03-22' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_exa_dim_productos', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
where bgdtrpp.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_bgdtrpp', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
and bgdtrpp.producto IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
	) A
WHERE A.ts_cue_actualizacion_calc= 1;

-- CALCULO EL FLAG DE UPGRADE LINEAL EN LAS NOVEDADES
DROP TABLE IF EXISTS paquete_novedad_con_flag;
create temporary table paquete_novedad_con_flag as
SELECT
  act.cod_suc_sucursal_paquete,
  act.cod_cue_cuenta,
  act.cod_cue_cuenta_paquete,
  act.cod_cue_producto_paquete,
  act.cod_cue_subproducto_paquete,
  act.dt_cue_upgrade,
  act.cod_cue_grupo_producto,
  ant.cod_cue_cuenta_paquete as cod_cue_cuenta_paquete_ant,
  ant.cod_cue_producto_paquete as cod_cue_producto_paquete_ant,
  ant.cod_cue_subproducto_paquete as cod_cue_subproducto_paquete_ant,
  ant.dt_cue_upgrade as dt_cue_upgrade_ant,
  act.cod_cue_estado_paquete,
  ant.cod_cue_grupo_producto as cod_cue_grupo_producto_ant,
  CASE WHEN cast(act.cod_cue_grupo_producto as int) = ant.cod_cue_grupo_producto and act.cod_cue_subproducto_paquete <> ant.cod_cue_subproducto_paquete then 1
       WHEN cast(act.cod_cue_grupo_producto as int) = ant.cod_cue_grupo_producto and act.cod_cue_subproducto_paquete = ant.cod_cue_subproducto_paquete and ant.flag_cue_upgrade_lineal=1 then 1
       else 0 end flag_cue_upgrade_lineal
FROM paquete_novedad act
LEFT JOIN bi_corp_common.stk_cue_paquetes_upgrade ant
on cast(act.cod_suc_sucursal_paquete as int) = ant.cod_suc_sucursal_paquete
and cast(act.cod_cue_cuenta  as bigint) = ant.cod_cue_cuenta
and partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}';

-- INSERTO LAS CUENTAS CON PAQUETES UGR Y LOS QUE NO SUFRIERON UGR
INSERT overwrite TABLE bi_corp_common.stk_cue_paquetes_upgrade
PARTITION(partition_date)
SELECT
	  cast(cod_suc_sucursal_paquete as int),
	  cast(cod_cue_cuenta as bigint),
	  cast(cod_cue_cuenta_paquete as bigint),
	  cod_cue_producto_paquete,
	  cod_cue_subproducto_paquete,
	  case when dt_cue_upgrade='0001-01-01' then null else dt_cue_upgrade end dt_cue_upgrade,
	  cast(cod_cue_grupo_producto as int),
	  cast(cod_cue_cuenta_paquete_ant as bigint),
	  cod_cue_producto_paquete_ant,
	  cod_cue_subproducto_paquete_ant,
	  case when dt_cue_upgrade_ant='0001-01-01' then null else dt_cue_upgrade_ant end dt_cue_upgrade_ant,
	  cast(cod_cue_grupo_producto_ant as int),
	  flag_cue_upgrade_lineal,
	  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' as partition_date
FROM paquete_novedad_con_flag
where cod_cue_estado_paquete='A' --paquetes activos

UNION ALL

SELECT
	  no_upg.cod_suc_sucursal_paquete,
	  no_upg.cod_cue_cuenta,
	  no_upg.cod_cue_cuenta_paquete,
	  no_upg.cod_cue_producto_paquete,
	  no_upg.cod_cue_subproducto_paquete,
	  case when no_upg.dt_cue_upgrade='0001-01-01' then null else no_upg.dt_cue_upgrade end dt_cue_upgrade,
	  no_upg.cod_cue_grupo_producto,
	  no_upg.cod_cue_cuenta_paquete_ant,
	  no_upg.cod_cue_producto_paquete_ant,
	  no_upg.cod_cue_subproducto_paquete_ant,
	  case when no_upg.dt_cue_upgrade_ant='0001-01-01' then null else no_upg.dt_cue_upgrade_ant end dt_cue_upgrade_ant,
	  no_upg.cod_cue_grupo_producto_ant,
	  no_upg.flag_cue_upgrade_lineal,
	  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' as partition_date
FROM bi_corp_common.stk_cue_paquetes_upgrade no_upg
left join paquete_novedad_con_flag upg
on no_upg.cod_cue_cuenta = cast(upg.cod_cue_cuenta as bigint)
and no_upg.cod_suc_sucursal_paquete = cast(upg.cod_suc_sucursal_paquete as int)
where no_upg.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
and upg.cod_cue_cuenta is null and upg.cod_suc_sucursal_paquete is null -- cargo las que no tuvieron update
;
