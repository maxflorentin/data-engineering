set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


--Calculamos el ultimo movimmiento genuino de la particion en ejecucion

DROP TABLE IF EXISTS ultimo_movimiento_genuino;
CREATE TEMPORARY TABLE ultimo_movimiento_genuino AS
SELECT  cod_cue_entidad,
		cod_suc_sucursal,
		cod_cue_cuenta,
		cod_cue_divisa,
		cod_cue_producto,
		cod_cue_subproducto,
		max(dt_cue_operacion) AS dt_cue_ultimo_movimiento_genuino
FROM bi_corp_common.bt_cue_movimientos_cuenta mov
WHERE mov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
	  AND flag_cue_genuino=1
GROUP BY
		cod_cue_entidad,
		cod_suc_sucursal,
		cod_cue_cuenta,
		cod_cue_divisa,
		cod_cue_producto,
		cod_cue_subproducto
;

--Calculamos el primer movimmiento genuino de la particion en ejecucion
DROP TABLE IF EXISTS primer_movimiento_genuino;
CREATE TEMPORARY TABLE primer_movimiento_genuino AS
SELECT  cod_cue_entidad,
		cod_suc_sucursal,
		cod_cue_cuenta,
		cod_cue_divisa,
		cod_cue_producto,
		cod_cue_subproducto,
		min(dt_cue_operacion) AS dt_cue_primer_movimiento_genuino
FROM bi_corp_common.bt_cue_movimientos_cuenta mov
WHERE mov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
	  AND flag_cue_genuino=1
GROUP BY
		cod_cue_entidad,
		cod_suc_sucursal,
		cod_cue_cuenta,
		cod_cue_divisa,
		cod_cue_producto,
		cod_cue_subproducto
;

--Calculamos la maxima particion de la tabla
DROP TABLE IF EXISTS bt_cue_movimientos_genuinos_max_particion;
CREATE TEMPORARY TABLE bt_cue_movimientos_genuinos_max_particion AS
select max(partition_date) as max_partition_date
from bi_corp_common.bt_cue_movimientos_genuinos
WHERE partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
	  and partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}',7);

--Calculamos la maxima particion de la tabla
DROP TABLE IF EXISTS malbgc_zbdtmig_max_particion;
CREATE TEMPORARY TABLE malbgc_zbdtmig_max_particion AS
select max(partition_date) as max_partition_date
from bi_corp_staging.malbgc_zbdtmig
WHERE partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
	  and partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}',7);

--Calculamos las cuentas que fueron migradas el dia de la ejecucion
DROP TABLE IF EXISTS cuentas_migradas;
CREATE TEMPORARY TABLE cuentas_migradas AS
select
	  A.mig_old_entidad as cod_cue_entidad_old,
	  A.mig_old_cent_alta as cod_suc_sucursal_old,
	  A.mig_old_cuenta as cod_cue_cuenta_old,
	  A.mig_old_divisa as cod_cue_divisa_old ,
	  A.mig_new_entidad as cod_cue_entidad,
	  A.mig_old_fech_baja as dt_cue_baja,
	  A.mig_new_cent_alta as cod_suc_sucursal,
	  A.mig_new_cuenta as cod_cue_cuenta,
	  A.mig_new_divisa as cod_cue_divisa
FROM bi_corp_staging.malbgc_zbdtmig A
inner join malbgc_zbdtmig_max_particion C
on A.partition_date=C.max_partition_date
and A.mig_old_fech_baja=C.max_partition_date
WHERE A.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
	  and A.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}',7);


--Calculamos el ultimo movimmiento y primer movimiento genuino del ultimo dia cargado de las cuentas que fueron migradas.
DROP TABLE IF EXISTS bt_cue_movimientos_genuinos_ult_part_migradas;
CREATE TEMPORARY TABLE bt_cue_movimientos_genuinos_ult_part_migradas AS
SELECT
	   C.cod_cue_cuenta,
	   C.cod_suc_sucursal,
	   C.cod_cue_divisa,
	   A.cod_cue_producto,
	   A.cod_cue_subproducto,
	   dt_cue_primer_movimiento_genuino,
	   dt_cue_ultimo_movimiento_genuino
FROM bi_corp_common.bt_cue_movimientos_genuinos A
inner join bt_cue_movimientos_genuinos_max_particion B
on A.partition_date = B.max_partition_date
inner join cuentas_migradas C
on  A.cod_suc_sucursal= cast(C.cod_suc_sucursal_old as int)
AND A.cod_cue_cuenta=cast(C.cod_cue_cuenta_old as bigint)
AND A.cod_cue_divisa=C.cod_cue_divisa_old
WHERE A.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
and A.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}',7);


--Calculamos el ultimo movimmiento y primer movimiento genuino del ultimo dia cargado de las cuentas que no fueron migradas.
DROP TABLE IF EXISTS bt_cue_movimientos_genuinos_ult_part_no_migradas;
CREATE TEMPORARY TABLE bt_cue_movimientos_genuinos_ult_part_no_migradas AS
SELECT
	   A.cod_cue_cuenta,
	   A.cod_suc_sucursal,
	   A.cod_cue_divisa,
	   A.cod_cue_producto,
	   A.cod_cue_subproducto,
	   dt_cue_primer_movimiento_genuino,
	   dt_cue_ultimo_movimiento_genuino
FROM bi_corp_common.bt_cue_movimientos_genuinos A
inner join bt_cue_movimientos_genuinos_max_particion B
on A.partition_date = B.max_partition_date
left join cuentas_migradas C
on  A.cod_suc_sucursal= cast(C.cod_suc_sucursal_old as int)
AND A.cod_cue_cuenta=cast(C.cod_cue_cuenta_old as bigint)
AND A.cod_cue_divisa=C.cod_cue_divisa_old
WHERE A.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
and A.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}',7)
and C.cod_cue_cuenta is null -- no migradas
;


--Unificamos las cuentas migradas y no migradas
DROP TABLE IF EXISTS bt_cue_movimientos_genuinos_ult_part;
CREATE TEMPORARY TABLE bt_cue_movimientos_genuinos_ult_part AS
SELECT
	   cast(cod_cue_cuenta as bigint) as  cod_cue_cuenta,
	   cast(cod_suc_sucursal as int) as cod_suc_sucursal,
	   cod_cue_divisa,
	   cod_cue_producto,
	   cod_cue_subproducto,
	   min(dt_cue_primer_movimiento_genuino) AS dt_cue_primer_movimiento_genuino,
	   max(dt_cue_ultimo_movimiento_genuino) AS dt_cue_ultimo_movimiento_genuino
FROM bt_cue_movimientos_genuinos_ult_part_no_migradas
GROUP BY
	   cast(cod_cue_cuenta as bigint),
	   cast(cod_suc_sucursal as int),
	   cod_cue_divisa,
	   cod_cue_producto,
	   cod_cue_subproducto
union all
SELECT
	   cast(cod_cue_cuenta as bigint) as  cod_cue_cuenta,
	   cast(cod_suc_sucursal as int) as cod_suc_sucursal,
	   cod_cue_divisa,
	   cod_cue_producto,
	   cod_cue_subproducto,
	   min(dt_cue_primer_movimiento_genuino) AS dt_cue_primer_movimiento_genuino,
	   max(dt_cue_ultimo_movimiento_genuino) AS dt_cue_ultimo_movimiento_genuino
FROM bt_cue_movimientos_genuinos_ult_part_migradas
GROUP BY
	   cast(cod_cue_cuenta as bigint),
	   cast(cod_suc_sucursal as int),
	   cod_cue_divisa,
	   cod_cue_producto,
	   cod_cue_subproducto
;

-- Las novedades por si la ultima particion de movimientos genuino tuvo alguna modificacion en su ultimo movimiento o hay que cargar el primero movimiento de alguno cuenta nueva
-- Cargo el ultimo movimiento y primer movimientos calculado con los datos de ayer en caso de que presenten
INSERT overwrite TABLE bi_corp_common.bt_cue_movimientos_genuinos
PARTITION(partition_date)
SELECT
	mae.entidad,
	mae.centro_alta,
	mae.cuenta,
	mae.divisa,
	mae.producto,
	mae.subprodu,
    min(coalesce(cmg.dt_cue_primer_movimiento_genuino,primer_movimiento_genuino.dt_cue_primer_movimiento_genuino)) as dt_cue_primer_movimiento_genuino,
	max(coalesce(ultimo_movimiento_genuino.dt_cue_ultimo_movimiento_genuino,cmg.dt_cue_ultimo_movimiento_genuino)) as dt_cue_ultimo_movimiento_genuino,
    mae.partition_date as partition_date
FROM bi_corp_staging.bgdtmae mae
LEFT JOIN ultimo_movimiento_genuino ultimo_movimiento_genuino
ON	CAST(mae.centro_alta AS int) = CAST(ultimo_movimiento_genuino.cod_suc_sucursal as int)
AND	CAST(mae.cuenta AS bigint) = CAST(ultimo_movimiento_genuino.cod_cue_cuenta as bigint)
--AND trim(mae.producto) = ultimo_movimiento_genuino.cod_cue_producto
--AND trim(mae.subprodu) = ultimo_movimiento_genuino.cod_cue_subproducto
AND	mae.divisa = ultimo_movimiento_genuino.cod_cue_divisa
LEFT JOIN primer_movimiento_genuino primer_movimiento_genuino
ON	CAST(mae.centro_alta AS int) = CAST(primer_movimiento_genuino.cod_suc_sucursal as int)
AND	CAST(mae.cuenta AS bigint) = CAST(primer_movimiento_genuino.cod_cue_cuenta as bigint)
--AND trim(mae.producto) = primer_movimiento_genuino.cod_cue_producto
--AND trim(mae.subprodu) = primer_movimiento_genuino.cod_cue_subproducto
AND	mae.divisa = primer_movimiento_genuino.cod_cue_divisa
LEFT JOIN bt_cue_movimientos_genuinos_ult_part cmg
ON	CAST(mae.centro_alta AS int) = cast(cmg.cod_suc_sucursal as int)
AND	CAST(mae.cuenta AS bigint) = cast( cmg.cod_cue_cuenta as bigint)
--AND trim(mae.producto) = cmg.cod_cue_producto
--AND trim(mae.subprodu) = cmg.cod_cue_subproducto
AND	mae.divisa = cmg.cod_cue_divisa
WHERE mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Hist_Mov_Genu') }}'
      and (cmg.dt_cue_primer_movimiento_genuino is not null or primer_movimiento_genuino.dt_cue_primer_movimiento_genuino is not null)
group by
    mae.entidad,
	mae.centro_alta,
	mae.cuenta,
	mae.divisa,
	mae.producto,
	mae.subprodu,
	mae.partition_date;