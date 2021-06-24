set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 4000000;
set hive.merge.smallfiles.avgsize = 16000000;
set hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.job.queue.name=root.dataeng;

--inserto transaccionalidad de acreditaciones
 INSERT
	OVERWRITE TABLE bi_corp_common.bt_acr_acreditaciones PARTITION (partition_date)
select
    sue.ent_empleado cod_acr_entidad_empleado,
	cast(sue.suc_empleado as bigint) cod_acr_suc_empleado,
	cast(sue.suc_fisica_empleado as bigint) cod_acr_suc_operativa_empleado,
	cast(sue.nro_empleado as bigint) cod_acr_cta_empleado,
	sue.div_empleado cod_div_divisa_empleado,
	sue.fecha_alta_cta_empleado dt_acr_alta_cta_empleado,
	case
	    when cast(sue.nup_empleado as bigint) = 0 then null
	    else cast(sue.nup_empleado as bigint) end cod_per_nup_empleado,
	case
	    when cast(trim(sue.cuil_empleado) as bigint) = 0 then null
	    else trim(sue.cuil_empleado) end ds_per_cuil_empleado,
	coalesce
	(case
		when trim(p1n.pepriape)= '' then trim(p1n.penomfan)
		else trim(concat(trim(p1n.pepriape), ' ', trim(p1n.penomper)))
	end,
	sue.nom_empleado) ds_per_nombre_empleado,
	sue.ent_empleador cod_acr_entidad_empleador,
	cast(sue.suc_empleador as bigint) cod_acr_suc_empleador,
	cast(sue.suc_fisica_empleador as bigint) cod_acr_suc_operativa_empleador,
	cast(sue.nro_empleador as bigint) cod_acr_cta_empleador,
	sue.div_empleador cod_div_divisa_empleador,
	sue.cbu_empleador cod_acr_cbu_empleador,
	case
	    when cast(sue.nup_empleador as bigint) = 0 then null
	    else cast(sue.nup_empleador as bigint) end cod_per_nup_empleador,
	case
	    when cast(trim(sue.cuit_empleador) as bigint) = 0 then null
	    else trim(sue.cuit_empleador) end ds_per_cuit_empleador,
	coalesce
	(case
		when trim(p1c.pepriape)= '' then trim(p1c.penomfan)
		else trim(concat(trim(p1c.pepriape), ' ', trim(p1c.penomper)))
	end,
	sue.nom_empleador) ds_per_nombre_empleador,
	cast (sue.suscriptor as bigint) cod_acr_suscriptor,
	sue.cod_convenio cod_acr_convenio,
	cast(sue.importe as float) fc_acr_importe,
	sue.fecha_acreditacion dt_acr_acreditacion,
	dtp.cod_acr_tipo_acred cod_acr_tipo_acreditacion,
	dtp.ds_acr_tipo_acred ds_acr_tipo_acreditacion,
	sue.sistema cod_acr_sistema,
	dtp.ds_acr_sistema ds_acr_sistema,
	sue.programa cod_acr_programa,
	sue.origen ds_acr_origen,
	sue.fecha_proceso partition_date
from
	bi_corp_staging.rio53_tplan_sueldo_ctas_dia sue
left join bi_corp_common.dim_acr_tipo_acred dtp on
	('2021-05-31' = dtp.partition_date
	and sue.sistema = dtp.cod_acr_sistema)
left join bi_corp_staging.malpe_pedt001 p1n on
	('2021-05-31' = p1n.partition_date
	and sue.nup_empleado = p1n.penumper)
left join bi_corp_staging.malpe_pedt001 p1c on
	('2021-05-31' = p1c.partition_date
	and sue.nup_empleador = p1c.penumper)
where
	sue.partition_date >= trunc('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones_Hist') }}','MM')
	and sue.partition_date <= last_day('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones_Hist') }}') ;