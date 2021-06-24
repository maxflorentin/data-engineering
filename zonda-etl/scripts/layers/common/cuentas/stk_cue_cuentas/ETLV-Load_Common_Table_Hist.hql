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
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
--AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where SUBSTRING(p.partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
AND p.pecalpar = 'TI';

DROP TABLE IF EXISTS pedt008_temp_sin_prod_act_1;
CREATE TEMPORARY TABLE pedt008_temp_sin_prod_act_1 AS
SELECT
	A.pecodofi,
	A.penumcon,
	A.pecodpro,
	A.pecodsub,
	A.penumper,
	A.pemotbaj
FROM pedt008_temp_sin_prod_act A
LEFT JOIN pedt008_temp_0 B
on A.pecodofi=B.pecodofi
and A.penumcon=B.penumcon
and A.pecodpro=B.producto_original
and A.pecodsub=B.subproducto_original
and A.penumper=B.penumper
WHERE B.penumper is null;


--------------Calculo la cantidad de firmantes por cuenta
DROP TABLE IF EXISTS cant_firmantes;
CREATE TEMPORARY TABLE cant_firmantes AS
	SELECT
			pecodofi
	       ,pecodpro
	       ,pecodsub
	       ,penumcon
	       ,count(DISTINCT penumper) AS fc_cue_cant_firmantes

	FROM
				(
				SELECT
						p08.penumper
				       ,p08.pecodofi
				       ,p08.pecodpro
				       ,p08.pecodsub
				       ,p08.penumcon
				       ,p08.pecodent
				       ,min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR

				FROM bi_corp_staging.malpe_pedt008 p08
				WHERE p08.pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
					  AND SUBSTRING(p08.partition_date,1,7) ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
				group by
						p08.penumper
				       ,p08.pecodofi
				       ,p08.pecodpro
				       ,p08.pecodsub
				       ,p08.penumcon
				       ,p08.pecodent
				) A
	GROUP BY
			pecodofi
	       ,pecodpro
	       ,pecodsub
	       ,penumcon;


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
	  AND genTabla='0311'
;

--------------Creo un tabla temporal con la ultima particion de Tarifas
DROP TABLE IF EXISTS tarifa_temp;
CREATE TEMPORARY TABLE tarifa_temp AS
SELECT trim(gen_clave) as cod_cue_tarifa,
	   trim(gen_datos) as ds_cue_tarifa
FROM bi_corp_staging.tcdtgen tcdtgen
WHERE  tcdtgen.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2020-01-08' then '2020-01-08' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
	  AND genTabla = '0435'
;


--------------Creo un tabla temporal con la ultima particion de CampaÃ±as
DROP TABLE IF EXISTS campania_temp;
CREATE TEMPORARY TABLE campania_temp AS
SELECT trim(gen_clave) as cod_cue_campania,
	   trim(gen_datos) as ds_cue_campania
FROM bi_corp_staging.tcdtgen tcdtgen
WHERE  tcdtgen.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' < '2020-01-08' then '2020-01-08' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' end
	  AND genTabla = '0517'
;

-------------- Sumatoria Acuerdos Especiales por Cuenta
DROP TABLE IF EXISTS acuerdos_especiales;
CREATE TEMPORARY TABLE acuerdos_especiales AS
SELECT
	acu_centro_alta as cod_suc_sucursal
	,acu_cuenta as cod_cue_cuenta
	,acu_divisa as cod_cue_divisa
	,sum(acu_limite) as Total_Acuerdos_Especiales
FROM
	bi_corp_staging.malbgc_bgdtacu
WHERE
	partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	and acu_fecha_inicio <='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	and acu_fecha_vcto >='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	and trim(acu_estado)='A'
group BY
	acu_centro_alta
	,acu_cuenta
	,acu_divisa
;

--Inserto los datos del stock de cuentas
INSERT overwrite TABLE bi_corp_common.stk_cue_cuentas
PARTITION(partition_date)
SELECT
    trim(mae.centro_alta) as cod_suc_sucursal
	,trim(mae.cuenta) as cod_cue_cuenta
	,coalesce(trim(pedt008.penumper),sin_prod_act.penumper) as cod_per_nup
	,trim(mae.producto) as cod_cue_producto
	,trim(mae.subprodu) as cod_cue_subproducto
	,coalesce(pedt008.pecodpro,trim(mae.producto)) as cod_cue_producto_actual
	,coalesce(pedt008.pecodsub,trim(mae.subprodu)) as cod_cue_subproducto_actual
	,trim(mae.divisa) as cod_cue_divisa
	,SUBSTRING(mae.cuenta,4,12)  AS cod_cue_altair
	,case when trim(mae.ind_codbloq)='S' then 1 else 0 end flag_cue_bloqueo
	,case when trim(mae.ind_sobregiro)='S' then 1 else 0 end flag_cue_sobregiro
	,case when trim(mae.ind_embargada)='S' then 1 else 0 end flag_cue_embargada
	,case when LENGTH(trim(mae.campania))=0 then null else trim(mae.campania) end cod_cue_campania
	,campania.ds_cue_campania
	,trim(mae.num_convenio) as cod_cue_nro_convenio
	,trim(mae.plan_comi) as cod_cue_plan_comision
	,plan.ds_cue_plan_comision
	,case when trim(mae.saldo_dispue_sgn)='-' THEN cast(concat(substring(mae.saldo_dispue,0,13),'.',substring(mae.saldo_dispue,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.saldo_dispue,0,13),'.',substring(mae.saldo_dispue,14,15)) AS decimal(18,2)) end fc_cue_saldo_dispuesto
	,case when trim(mae.saldo_impago_sgn)='-' THEN cast(concat(substring(mae.saldo_impago,0,13),'.',substring(mae.saldo_impago,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.saldo_impago,0,13),'.',substring(mae.saldo_impago,14,15)) AS decimal(18,2)) end fc_cue_saldo_impago
	,case when trim(mae.disp_total_acu_sgn)='-' THEN cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2)) end fc_cue_disponible_total_acuerdos
	,case when trim(mae.saldo_dispue_ca_sgn)='-' THEN cast(concat(substring(mae.saldo_dispue_ca,0,13),'.',substring(mae.saldo_dispue_ca,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.saldo_dispue_ca,0,13),'.',substring(mae.saldo_dispue_ca,14,15)) AS decimal(18,2)) end fc_cue_saldo_caja_ahorro
	,case when trim(mae.saldo_dispue_cc_sgn)='-' THEN cast(concat(substring(mae.saldo_dispue_cc,0,13),'.',substring(mae.saldo_dispue_cc,14,15)) AS decimal(18,2))*-1
		  else cast(concat(substring(mae.saldo_dispue_cc,0,13),'.',substring(mae.saldo_dispue_cc,14,15)) AS decimal(18,2)) end fc_cue_saldo_cuenta_corriente
	,aux.fecha_alta as dt_cue_alta
	,mae.ind_posic_cta as cod_cue_posicion_cuenta
	,trim(mae.indsitu_ob) as cod_cue_situacion_observado
	,paquetes_upgrade.cod_cue_cuenta_paquete
	,CASE WHEN paquetes_upgrade.cod_cue_cuenta is null then 0 else 1 end AS flag_cue_paquete
	,paquetes_upgrade.cod_cue_producto_paquete
	,paquetes_upgrade.cod_cue_subproducto_paquete
	,case when cast(mae.centro_alta as int) between 250 and 259 then 1 else 0 end as flag_cue_banca_privada
	,cant_firmantes.fc_cue_cant_firmantes
	,trim(mae.indesta) as cod_cue_estado_cuenta
	,movimientos_genuinos.dt_cue_primer_movimiento_genuino
	,movimientos_genuinos.dt_cue_ultimo_movimiento_genuino
	,(case when mae.disp_total_acu is null then 0
	      when trim(mae.disp_total_acu_sgn)='-' THEN cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2))*-1
		       else cast(concat(substring(mae.disp_total_acu,0,13),'.',substring(mae.disp_total_acu,14,15)) AS decimal(18,2)) end) - acu.Total_Acuerdos_Especiales fc_cue_monto_acuerdo_comun	-- Faltaria restarle el especial
    ,case when trim(paquetes_upgrade.dt_cue_upgrade)='0001-01-01' then null else trim(paquetes_upgrade.dt_cue_upgrade) end  AS dt_cue_upgrade
    ,case when trim(paquetes_upgrade.dt_cue_upgrade)='0001-01-01' then null else coalesce(paquetes_upgrade.flag_cue_upgrade_lineal,0) end as flag_cue_upgrade_lineal
	,NULL AS flag_cue_marca_contrato_citi
	,garra_contratos.waguxdex_cod_marca as cod_cue_marca
	,garra_contratos.waguxdex_cod_smarcgsi as cod_cue_submarca
	,case when trim(garra_contratos.waguxdex_fec_incumplim)='0001-01-01' is null then trim(garra_contratos.waguxdex_fec_incumplim) end as dt_cue_irregularidad
	,case when garra_contratos.waguxdex_imp_riesmolo is null then 0 else garra_contratos.waguxdex_imp_riesmolo end  as fc_cue_importe_riesgo
	,case when garra_contratos.waguxdex_imp_irremolo is null then 0 else garra_contratos.waguxdex_imp_irremolo end as fc_cue_importe_irregular
	,aux.tarifa as cod_cue_tarifa
	,tarifa.ds_cue_tarifa
	,trim(mae.fecha_ultope) as dt_cue_ultimo_tope
	,trim(aux.pco_cop_ind) as cod_cue_pco_cop
	,trim(aux.pco_ecu_sop) as cod_cue_pco_ecu
	,CASE WHEN trim(aux.pco_cop_ind)='S' AND trim(aux.pco_ecu_sop)='B' THEN 1 ELSE 0 END flag_cue_resumen_online
	,CASE WHEN LENGTH(trim(pedt042.pecanven))=0 THEN NULL ELSE trim(pedt042.pecanven) END AS cod_cue_canal
	,dim_cue_canales.ds_cue_canal_new
	,CASE WHEN trim(aux.fecha_cierre) IN ('9999-12-31', '0001-01-01') THEN NULL ELSE trim(aux.fecha_cierre) END AS dt_cue_baja
	,CASE WHEN LENGTH(trim(pedt008.pemotbaj))=0 THEN NULL ELSE trim(pedt008.pemotbaj) END AS cod_cue_motivo_baja
	,case when length(trim(pedt042.pesucope))=0 then null else trim(pedt042.pesucope) end cod_suc_sucursal_operativa
	,mae.partition_date
FROM bi_corp_staging.bgdtmae mae
LEFT JOIN bi_corp_staging.bgdtaux aux
ON	mae.entidad = aux.entidad
AND	mae.centro_alta = aux.centro_alta
AND	mae.cuenta = aux.cuenta
AND	mae.divisa = aux.divisa
AND	aux.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
LEFT JOIN pedt008_temp pedt008
ON mae.centro_alta = pedt008.pecodofi
AND CAST(SUBSTRING(mae.cuenta,4) AS bigint) = pedt008.penumcon
AND trim(mae.producto) = pedt008.producto_original
AND trim(mae.subprodu) = pedt008.subproducto_original
LEFT JOIN pedt008_temp_sin_prod_act_1 sin_prod_act
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
LEFT JOIN bi_corp_staging.garra_wagucdex garra_contratos
ON	mae.entidad = garra_contratos.waguxdex_cod_entidad
AND	mae.centro_alta = garra_contratos.waguxdex_cod_centro
AND	CAST(SUBSTRING(mae.cuenta,4) AS bigint) = CAST(garra_contratos.waguxdex_num_contrato AS bigint)
AND	mae.divisa = garra_contratos.waguxdex_cod_divisa
AND coalesce(pedt008.pecodpro,trim(mae.producto)) = garra_contratos.waguxdex_cod_producto
AND coalesce(pedt008.pecodsub,trim(mae.subprodu)) = garra_contratos.waguxdex_cod_subprodu
AND garra_contratos.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_garra_wagucdex', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
LEFT JOIN bi_corp_common.dim_cue_canales dim_cue_canales
ON trim(pedt042.pecanven) = dim_cue_canales.cod_cue_canal
LEFT JOIN cant_firmantes cant_firmantes
ON CAST(SUBSTRING(mae.cuenta,4) AS bigint) = cast(cant_firmantes.penumcon AS bigint)
and mae.centro_alta = cant_firmantes.pecodofi
AND trim(mae.producto) = cant_firmantes.pecodpro
AND trim(mae.subprodu) = cant_firmantes.pecodsub
LEFT JOIN bi_corp_common.bt_cue_movimientos_genuinos movimientos_genuinos
ON	cast(mae.entidad as int) = movimientos_genuinos.cod_cue_entidad
AND	cast(mae.centro_alta as int) = movimientos_genuinos.cod_suc_sucursal
AND	CAST(mae.cuenta AS bigint) = movimientos_genuinos.cod_cue_cuenta
AND	mae.divisa = movimientos_genuinos.cod_cue_divisa
AND trim(mae.producto) = movimientos_genuinos.cod_cue_producto
AND trim(mae.subprodu) = movimientos_genuinos.cod_cue_subproducto
AND movimientos_genuinos.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}' 
LEFT JOIN bi_corp_common.stk_cue_paquetes_upgrade paquetes_upgrade
ON  cast(mae.centro_alta as int) = paquetes_upgrade.cod_suc_sucursal_paquete
AND cast(mae.cuenta AS bigint)  = paquetes_upgrade.cod_cue_cuenta
AND paquetes_upgrade.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
LEFT JOIN acuerdos_especiales acu
ON mae.divisa = acu.cod_cue_divisa
AND mae.centro_alta = acu.cod_suc_sucursal
AND mae.cuenta = acu.cod_cue_cuenta
WHERE mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Ondemand') }}'
	  AND (mae.indesta = 'A' OR pedt008.pecodpro='70') -- Logica que indica stock
;