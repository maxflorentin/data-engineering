set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente sin Mora
DROP TABLE IF EXISTS pedt008_temp_sin_mora;
CREATE TEMPORARY TABLE pedt008_temp_sin_mora AS
select p.pecodofi, cast(p.penumcon as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where SUBSTRING(partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where SUBSTRING(p.partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND p.pecalpar = 'TI';


--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente con Mora
DROP TABLE IF EXISTS pedt008_temp_con_mora;
CREATE TEMPORARY TABLE pedt008_temp_con_mora AS
select p.pecodofi, cast(SUBSTRING(p.penumcon,4) as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where SUBSTRING(partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('70')
AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where SUBSTRING(p.partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND p.pecalpar = 'TI';

DROP TABLE IF EXISTS pedt008_prod_ori;
CREATE TEMPORARY TABLE pedt008_prod_ori AS
SELECT  pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		b.codigo_producto AS producto_original,
		b.codigo_subproducto AS subproducto_original
FROM pedt008_temp_con_mora a
LEFT JOIN bi_corp_staging.risksql5_productos b
ON a.pecodpro = b.codigo_producto_mora
AND a.pecodsub = b.codigo_subproducto_mora
AND b.fecha_informacion = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2021-05-04' then '2021-05-04' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_risksql5_productos', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end

GROUP BY
     	pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		b.codigo_producto,
		b.codigo_subproducto;

------------ Universo de cuentas/cliente sin mora o con mora con el producto original
DROP TABLE IF EXISTS pedt008_temp_0;
CREATE TEMPORARY TABLE pedt008_temp_0 AS
SELECT
        pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		pecodpro as producto_original,
		pecodsub as subproducto_original
FROM
    pedt008_temp_sin_mora
union all
SELECT
        pecodofi,
		penumcon,
		pecodpro,
		pecodsub,
		penumper,
		pefecalt,
		cuenta,
		pemotbaj,
		producto_original,
		subproducto_original
FROM
    pedt008_prod_ori;

------------ Calculo la maxima fecha de alta de las cuentas-clientes
DROP TABLE IF EXISTS pedt008_temp_1;
CREATE TEMPORARY TABLE pedt008_temp_1 AS
select pecodofi,penumcon,producto_original,subproducto_original, max(pefecalt) pefecalt
from pedt008_temp_0
group by pecodofi,penumcon,producto_original,subproducto_original;


------------ Me quedo con las cuentas activas que tenga la mayor fecha de alta y el ultimo producto
DROP TABLE IF EXISTS pedt008_temp_2;
CREATE TEMPORARY TABLE pedt008_temp_2 AS
select A.pecodofi,
		A.penumcon,
		A.producto_original,
		A.subproducto_original,
		A.pefecalt,
	    A.penumper,
	    A.pemotbaj,
		A.pecodsub,
		A.pecodpro
from pedt008_temp_0 A
inner join pedt008_temp_1 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.producto_original=B.producto_original
and A.subproducto_original=B.subproducto_original
and A.pefecalt=B.pefecalt;

DROP TABLE IF EXISTS pedt008_temp_3;
CREATE TEMPORARY TABLE pedt008_temp_3 AS
select pecodofi,penumcon,producto_original,subproducto_original, max(pecodpro) pecodpro
from pedt008_temp_2
group by pecodofi,penumcon,producto_original,subproducto_original;


DROP TABLE IF EXISTS pedt008_temp;
CREATE TEMPORARY TABLE pedt008_temp AS
select A.pecodofi,
		A.penumcon,
		A.producto_original,
		A.subproducto_original,
		A.pefecalt,
	    A.penumper,
	    A.pemotbaj,
		A.pecodsub,
		A.pecodpro
from pedt008_temp_2 A
inner join pedt008_temp_3 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.producto_original=B.producto_original
and A.subproducto_original=B.subproducto_original
and A.pecodpro=B.pecodpro;


--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente que en la tabla mae está activo pero no en la pedt008
DROP TABLE IF EXISTS pedt008_temp_sin_prod_act;
CREATE TEMPORARY TABLE pedt008_temp_sin_prod_act AS
select p.pecodofi, cast(p.penumcon as bigint) as penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta,pemotbaj
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where SUBSTRING(partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14', '70')
--AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where SUBSTRING(p.partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND p.pecalpar = 'TI';


--------------Creo un tabla temporal con la ultima particion de los datos de Canal de Venta - Cliente
DROP TABLE IF EXISTS pedt042_temp;
CREATE TEMPORARY TABLE pedt042_temp AS
SELECT p042.*
FROM bi_corp_staging.malpe_pedt042 p042
WHERE p042.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2019-12-04' then '2019-12-04' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
;


--------------Creo un tabla temporal con la ultima particion de Plan Comision
DROP TABLE IF EXISTS plancomision_temp;
CREATE TEMPORARY TABLE plancomision_temp AS
SELECT trim(gen_clave) as cod_cue_plan_comision,
	   trim(gen_datos) as ds_cue_plan_comision
FROM bi_corp_staging.tcdtgen tcdtgen
WHERE  tcdtgen.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2020-01-08' then '2020-01-08' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
	  AND genTabla='0311';

--------------Creo un tabla temporal con la ultima particion de Tarifas
DROP TABLE IF EXISTS tarifa_temp;
CREATE TEMPORARY TABLE tarifa_temp AS
SELECT trim(gen_clave) as cod_cue_tarifa,
	   trim(gen_datos) as ds_cue_tarifa
FROM bi_corp_staging.tcdtgen tcdtgen
WHERE  tcdtgen.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2020-01-08' then '2020-01-08' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
	  AND genTabla = '0435';

--------------Creo un tabla temporal con la ultima particion de CampaÃ±as
DROP TABLE IF EXISTS campania_temp;
CREATE TEMPORARY TABLE campania_temp AS
SELECT trim(gen_clave) as cod_cue_campania,
	   trim(gen_datos) as ds_cue_campania
FROM bi_corp_staging.tcdtgen tcdtgen
WHERE  tcdtgen.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2020-01-08' then '2020-01-08' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
	  AND genTabla = '0517';

DROP TABLE IF EXISTS stock;
CREATE TEMPORARY TABLE stock AS
select 
	 trim(mae.centro_alta) as cod_suc_sucursal
	,trim(mae.cuenta) as cod_cue_cuenta
	,trim(mae.producto) as cod_cue_producto
	,trim(mae.subprodu) as cod_cue_subproducto
	,trim(mae.divisa) as cod_cue_divisa
FROM bi_corp_staging.bgdtmae mae
LEFT JOIN pedt008_temp pedt008
ON mae.centro_alta = pedt008.pecodofi
AND CAST(SUBSTRING(mae.cuenta,4) AS bigint) = pedt008.penumcon
AND trim(mae.producto) = pedt008.producto_original
AND trim(mae.subprodu) = pedt008.subproducto_original
WHERE mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	  AND (mae.indesta = 'A' OR pedt008.pecodpro='70') -- Logica que indica stock
;
--Inserto los datos del stock de cuentas dadas de bajas
INSERT overwrite TABLE bi_corp_common.stk_cue_bajas
PARTITION(partition_date)
SELECT
    trim(mae.centro_alta) as cod_suc_sucursal
	,trim(mae.cuenta) as cod_cue_cuenta
	,trim(sin_prod_act.penumper) as cod_per_nup
	,trim(mae.producto) as cod_cue_producto
	,trim(mae.subprodu) as cod_cue_subproducto
	,trim(sin_prod_act.pecodpro) as cod_cue_producto_actual
	,trim(sin_prod_act.pecodsub) as cod_cue_subproducto_actual
	,trim(mae.divisa) as cod_cue_divisa
	,SUBSTRING(mae.cuenta,4,12)  AS cod_cue_altair
	,case when trim(mae.ind_codbloq)='S' then 1 else 0 end flag_cue_bloqueo
	,case when LENGTH(trim(mae.campania))=0 then null else trim(mae.campania) end cod_cue_campania
	,campania.ds_cue_campania
	,trim(mae.num_convenio) as cod_cue_nro_convenio
	,trim(mae.plan_comi) as cod_cue_plan_comision
	,plan.ds_cue_plan_comision
	,case when trim(mae.saldo_dispue_sgn)='-' THEN cast(concat(substring(mae.saldo_dispue,0,13),'.',substring(mae.saldo_dispue,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.saldo_dispue,0,13),'.',substring(mae.saldo_dispue,14,15)) AS decimal(18,2)) end fc_cue_saldo_dispuesto
	,case when trim(mae.disp_total_acu_sgn)='-' THEN cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2)) end fc_cue_disponible_total_acuerdos
	,aux.fecha_alta as dt_cue_alta
	,paquetes_upgrade.cod_cue_cuenta_paquete
	,CASE WHEN paquetes_upgrade.cod_cue_cuenta is null then 0 else 1 end AS flag_cue_paquete
	,paquetes_upgrade.cod_cue_producto_paquete
	,paquetes_upgrade.cod_cue_subproducto_paquete
    ,case when trim(paquetes_upgrade.dt_cue_upgrade)='0001-01-01' then null else trim(paquetes_upgrade.dt_cue_upgrade) end  AS dt_cue_upgrade
    ,case when trim(paquetes_upgrade.dt_cue_upgrade)='0001-01-01' then null else coalesce(paquetes_upgrade.flag_cue_upgrade_lineal,0) end as flag_cue_upgrade_lineal
	,NULL AS flag_cue_marca_contrato_citi
	,aux.tarifa as cod_cue_tarifa
	,tarifa.ds_cue_tarifa
	,CASE WHEN trim(aux.fecha_cierre) IN ('9999-12-31', '0001-01-01') THEN NULL ELSE trim(aux.fecha_cierre) END AS dt_cue_baja
	,CASE WHEN LENGTH(trim(pedt042.pecanven))=0 THEN NULL ELSE trim(pedt042.pecanven) END AS cod_cue_canal
	,dim_cue_canales.ds_cue_canal
	,CASE WHEN LENGTH(trim(sin_prod_act.pemotbaj))=0 THEN NULL ELSE trim(sin_prod_act.pemotbaj) END AS cod_cue_motivo_baja
	,case when length(trim(pedt042.pesucope))=0 then null else trim(pedt042.pesucope) end cod_suc_sucursal_operativa
	,mae.partition_date
FROM bi_corp_staging.bgdtmae mae
LEFT JOIN bi_corp_staging.bgdtaux aux
ON	mae.entidad = aux.entidad
AND	mae.centro_alta = aux.centro_alta
AND	mae.cuenta = aux.cuenta
AND	mae.divisa = aux.divisa
AND	aux.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
LEFT JOIN pedt008_temp_sin_prod_act sin_prod_act
ON mae.centro_alta = sin_prod_act.pecodofi
AND CAST(SUBSTRING(mae.cuenta,4) AS bigint) = sin_prod_act.penumcon
AND trim(mae.producto) = sin_prod_act.pecodpro
AND trim(mae.subprodu) = sin_prod_act.pecodsub
LEFT JOIN pedt042_temp pedt042
ON	mae.entidad = pedt042.pecodent
AND	mae.centro_alta = pedt042.pecodofi
AND	mae.cuenta = concat('0', pedt042.pecodpro, SUBSTRING(pedt042.penumcon, 4))
AND	mae.divisa = pedt042.pecodmon
LEFT JOIN plancomision_temp plan
ON trim(mae.plan_comi)=plan.cod_cue_plan_comision
LEFT JOIN tarifa_temp tarifa
ON trim(aux.tarifa)=tarifa.cod_cue_tarifa
LEFT JOIN campania_temp campania
ON trim(mae.campania)=campania.cod_cue_campania
LEFT JOIN bi_corp_common.dim_cue_canales dim_cue_canales
ON trim(pedt042.pecanven) = dim_cue_canales.cod_cue_canal
LEFT JOIN bi_corp_common.stk_cue_paquetes_upgrade paquetes_upgrade
ON mae.centro_alta = paquetes_upgrade.cod_suc_sucursal_paquete
AND mae.cuenta = paquetes_upgrade.cod_cue_cuenta
AND paquetes_upgrade.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
LEFT JOIN stock stk
ON	mae.centro_alta = stk.cod_suc_sucursal
AND	mae.cuenta = stk.cod_cue_cuenta
AND	mae.divisa = stk.cod_cue_divisa
AND trim(mae.producto) = stk.cod_cue_producto
AND trim(mae.subprodu) = stk.cod_cue_subproducto
WHERE mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	  AND stk.cod_cue_cuenta is null -- obtengo las bajas
;

