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

--inserto stk con todas las empresas con sus respectivos convenios plan sueldo
 INSERT
	OVERWRITE TABLE bi_corp_common.stk_acr_conv_empresas PARTITION (partition_date)
select distinct
	cast(b001.`001_suscriptor` as bigint) cod_acr_suscriptor,
	b001.`001_des_suscriptor` ds_acr_suscriptor,
	cast(mco.mco_num_convenio as bigint) cod_acr_num_convenio,
	mco.mco_convenio cod_acr_convenio,
	b001.`001_tpo_suscriptor` cod_acr_tipo_suscriptor,
	cast(null as string) ds_acr_tipo_suscriptor,
	cast(p8.penumper as bigint) cod_per_nup,
	p15.penumdoc ds_per_cuit,
	b001.`001_entidad` cod_acr_entidad,
	cast(b001.`001_centro_alta` as bigint) cod_acr_sucursal,
	cast(b001.`001_cuenta` as bigint) cod_acr_cuenta,
	b001.`001_divisa` cod_div_divisa,
	mco.mco_producto cod_prod_producto_contab,
	mco.mco_subprodu cod_prod_subproducto_contab,
	mco.mco_indesta cod_acr_estado,
	mco.mco_fecha_est dt_acr_estado,
	pab.pab_concepto cod_acr_concepto,
	pab.pab_porc_suscriptor fc_acr_porc_suscriptor,
	pab.pab_porc_entidad fc_acr_porc_entidad,
	pab.pab_porc_cliente fc_acr_porc_cliente,
	case
		when trim(b001.`001_entidad_umo`) = '' then cast(null as string)
		else b001.`001_entidad_umo`
	end cod_acr_tbgb_entidad_umo,
	case
		when trim(b001.`001_centro_umo`) = '' then cast(null as string)
		else cast(b001.`001_centro_umo` as bigint)
	end cod_acr_tbgb_centro_umo,
	case
		when trim (b001.`001_userid_umo`) = '' then cast(null as string)
		else b001.`001_userid_umo`
	end cod_acr_tbgb_userid_umo,
	case
		when trim (b001.`001_netname_umo`) = '' then cast(null as string)
		else b001.`001_netname_umo`
	end cod_acr_tbgb_netname_umo,
	case
		when trim (b001.`001_timest_umo`) = '' then cast(null as string)
		else b001.`001_timest_umo`
	end ts_acr_tbgb_timest_umo,
	case
		when trim (mco.mco_entidad_umo) = '' then cast(null as string)
		else mco.mco_entidad_umo
	end cod_acr_mco_entidad_umo,
	case
		when trim (mco_centro_umo) = '' then cast(null as string)
		else cast(mco_centro_umo as bigint)
	end cod_acr_mco_centro_umo,
	case
		when trim (mco.mco_userid_umo) = '' then cast(null as string)
		else mco.mco_userid_umo
	end cod_acr_mco_userid_umo,
	case
		when trim (mco.mco_netname_umo) = '' then cast(null as string)
		else mco.mco_netname_umo
	end cod_acr_mco_netname_umo,
	case
		when trim (mco.mco_timest_umo) = '' then cast(null as string)
		else mco.mco_timest_umo
	end ts_acr_mco_timest_umo,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}' partition_date
from
	bi_corp_staging.malbgc_tbgb001 b001
inner join bi_corp_staging.malbgc_bgdtmco mco on
	(b001.partition_date = mco.partition_date
	and b001.`001_suscriptor` = mco.mco_suscriptor)
left join bi_corp_staging.malbgc_bgdtpab pab on
	(mco.mco_num_convenio = pab.pab_num_convenio
	and pab.pab_concepto = 'MANT'
	and mco.partition_date = pab.partition_date)
left join TEMP_PEDT008_CTAS p8 on
	(concat('000', substr(b001.`001_cuenta`, 4)) = p8.penumcon
	and b001.`001_centro_alta` = p8.pecodofi)
left join TEMP_PEDT015 p15 on
	(p8.penumper = p15.penumper)
where
	b001.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}';