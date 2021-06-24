set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 4000000;
set hive.merge.smallfiles.avgsize = 16000000;
set hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.job.queue.name=root.dataeng;

--armo tabla temporal PEDT008 con todos los contratos de cuentas para obtener nup por cuenta/sucursal sin duplicados
 CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT008_CTAS as
select
	p8.penumper,
	p8.penumcon,
	p8.pecodofi
from
	(
	select
		p8a.penumper,
		p8a.penumcon,
		p8a.pecodofi,
		row_number() over (partition by p8a.penumcon,
		p8a.pecodofi
	order by
		coalesce(p8a.pefecbrb,
		'9999-12-31') desc,
		p8a.peordpar asc,
		p8a.penumper ) as row_num
	from
		bi_corp_staging.malpe_pedt008 p8a
	where
		p8a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}'
		and p8a.pecalpar = 'TI'
		and p8a.pecodpro IN ('02', '03', '05', '06', '07', '14', '98', '99') ) p8
where
	p8.row_num = 1;

--armo tabla temporal PEDT015 con todos los nups sin duplicados con prioridad por cuit/cuil
 CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT015 as
select
	p.penumper,
	p.penumdoc
from
	(
	select
		p15.penumper,
		p15.penumdoc,
		row_number() over (partition by p15.penumper
	order by
		p15.petipdoc,
		p15.penumdoc ) as row_num
	from
		(
		select
			p15a.penumper,
			p15a.penumdoc,
			case
				when trim(p15a.petipdoc) = 'T' then 1
				when trim(p15a.petipdoc) = 'L' then 2
				when trim(p15a.petipdoc) = 'D' then 3
				else 4
			end petipdoc
		from
			bi_corp_staging.malpe_pedt015 p15a
		where
			p15a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}'
			) p15 )p
where
	p.row_num = 1;

-- armo tabla temporal para obtener los producto/subproducto paquete ya que en bgdtmae no esta actualizado
 CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PAQ as
select
	cod_suc_sucursal_paquete,
	cod_cue_cuenta,
	cod_cue_producto_paquete,
	cod_cue_subproducto_paquete
from
	(
	select
		bgdtrpp.centro_alta2 as cod_suc_sucursal_paquete,
		bgdtrpp.cuenta as cod_cue_cuenta,
		bgdtmpa.mpa_producto_paq as cod_cue_producto_paquete,
		bgdtmpa.mpa_subprodu_paq as cod_cue_subproducto_paquete,
		row_number() over (PARTITION BY bgdtrpp.centro_alta2,
		bgdtrpp.cuenta
	order by
		estarel asc,
		timest_umo desc) as ts_cue_actualizacion_calc
	from
		bi_corp_staging.malbgc_bgdtrpp bgdtrpp
	LEFT JOIN bi_corp_staging.malbgc_bgdtmpa bgdtmpa ON
		bgdtrpp.entidad = bgdtmpa.mpa_entidad
		AND bgdtrpp.centro_alta = bgdtmpa.mpa_centro_alta
		AND bgdtrpp.paquete = bgdtmpa.mpa_paquete
		AND bgdtrpp.partition_date = bgdtmpa.partition_date
	where
		bgdtrpp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}'
		and bgdtrpp.producto IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
		and bgdtrpp.estarel = 'A' ) A
WHERE
	A.ts_cue_actualizacion_calc = 1;

--inserto stk con todas las cuentas con convenio plan sueldo
 INSERT
	OVERWRITE TABLE bi_corp_common.stk_acr_conv_empleados PARTITION (partition_date)
select distinct
	cast(b001.`001_suscriptor` as bigint) cod_acr_suscriptor,
	cast(mco.mco_num_convenio as bigint) cod_acr_num_convenio,
	mco.mco_convenio cod_acr_convenio,
	b001.`001_tpo_suscriptor` cod_acr_tipo_suscriptor,
	null ds_acr_tipo_suscriptor,
	cast(p8.penumper as bigint) cod_per_nup,
	p15.penumdoc ds_per_cuit,
	mae.entidad cod_acr_entidad,
	cast(mae.centro_alta as bigint) cod_acr_sucursal,
	cast(mae.cuenta as bigint) cod_acr_cuenta,
	mae.divisa cod_div_divisa,
	mae.producto cod_prod_producto,
	mae.subprodu cod_prod_subproducto,
	paq.cod_cue_producto_paquete cod_prod_producto_contab,
	paq.cod_cue_subproducto_paquete cod_prod_subproducto_contab,
	mco.mco_indesta cod_acr_estado,
	mco.mco_fecha_est dt_acr_estado,
	case
		when trim(mae.entidad_umo) = '' then null
		else trim(mae.entidad_umo)
	end cod_acr_mae_entidad_umo,
	case
		when trim(mae.centro_umo) = '' then null
		else cast(mae.centro_umo as bigint)
	end cod_acr_mae_centro_umo,
	case
		when trim(mae.userid_umo) = '' then null
		else trim(mae.userid_umo)
	end cod_acr_mae_userid_umo,
	case
		when trim(netname_umo) = '' then null
		else trim(netname_umo)
	end cod_acr_mae_netname_umo,
	case
		when trim(timest_umo) = '' then null
		else trim(timest_umo)
	end cod_acr_mae_timest_umo,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}' partition_date
from
	bi_corp_staging.bgdtmae mae
inner join bi_corp_staging.malbgc_bgdtmco mco on
	(cast(mae.num_convenio as bigint) = mco.mco_num_convenio
	and mae.partition_date = mco.partition_date)
inner join bi_corp_staging.malbgc_tbgb001 b001 on
	(b001.partition_date = mco.partition_date
	and b001.`001_suscriptor` = mco.mco_suscriptor)
left join TEMP_PEDT008_CTAS p8 on
	(concat('000', substr(mae.cuenta, 4)) = p8.penumcon
	and mae.centro_alta = p8.pecodofi)
left join TEMP_PEDT015 p15 on
	(p8.penumper = p15.penumper)
left join TEMP_PAQ paq on
    (mae.cuenta = paq.cod_cue_cuenta
    and mae.centro_alta = paq.cod_suc_sucursal_paquete)
where
	mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}'
	and mae.indesta = 'A';