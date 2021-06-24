SET APPX_COUNT_DISTINCT = TRUE;
SET hive.merge.mapfiles = TRUE;
SET hive.merge.mapredfiles = TRUE;
SET hive.merge.size.per.task = 4000000;
SET hive.merge.smallfiles.avgsize = 16000000;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.job.queue.name = root.dataeng;
DROP TABLE IF EXISTS working_days;
create temporary table working_days as
	select
		row_number() over (order by row_num) ind,
		row_num,
		to_date(fec_yyyy_mm_dd) working_day
	from (
		select
		*, rank() over (partition by row_num order BY data_date_part desc) rnk
		from santander_business_risk_arda.calendario where
		to_date(fec_yyyy_mm_dd) >= to_date('2020-01-01') and
		to_date(fec_yyyy_mm_dd) <= to_date('2025-01-01')
	) a where rnk = 1 and flag_laborable = 1 and fec_yyyy_mm_dd not like '%-11-06';
	DROP TABLE IF EXISTS shippings;
create temporary table shippings as
	select a.cod_deli_crupier,a.dt_deli_registro,a.cod_deli_producto,a.cod_deli_operacion,a.cod_deli_paquete,a.ds_deli_operacion,
			a.ds_deli_paquete_crupier from (
		select
			e.cod_deli_crupier,e.dt_deli_registro,e.cod_deli_producto,e.cod_deli_operacion,e.cod_deli_paquete,e.ds_deli_operacion,e.ds_deli_paquete_crupier,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_envios e
		where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro > to_date('2020-07-19')
	)a where rnk = 1;
	DROP TABLE IF EXISTS shippings_couriers;
create temporary table shippings_couriers as
	select a.cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking from (
		select
			e.cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking,
			rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_courier_shipping e
		where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and TO_DATE(to_utc_timestamp(e.ts_deli_creacion,'MSK')) > to_date('2020-07-19')
	)a where rnk = 1;
	DROP TABLE IF EXISTS shippings_subproductos;
create temporary table shippings_subproductos as
	select cast(crupier_id as int) cod_deli_crupier,cod_subproducto from (
		select
			crupier_id,cod_subproducto,
			rank() over (partition by crupier_id order BY to_date(ultima_modif) desc) rnk
		from bi_corp_staging.rio258_cp_envios e where cod_subproducto is not null and cod_subproducto <> 'null'
	)a where rnk = 1;
	DROP TABLE IF EXISTS sequences;
create temporary table sequences as
	select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping from (
		select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping,
			rank() over (partition by cod_deli_codigo order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_courier_component --where cod_deli_codigo is not null and cod_deli_codigo <> 'null'
	) a where rnk = 1;
	DROP TABLE IF EXISTS completed_state;
create temporary table completed_state as
	select a.cod_deli_crupier,max(dt_deli_novedad_estado) as completed from (
		select
			rank() over (partition by cod_deli_componente_envio order BY to_date(ce.partition_date) desc) rnk,
		    dt_deli_novedad_estado,
		    c.cod_deli_crupier
		from shippings e
		left join bi_corp_common.bt_deli_componentes c on e.cod_deli_crupier = c.cod_deli_crupier
		left join bi_corp_common.bt_deli_componentes_estados ce on c.cod_deli_componente=ce.cod_deli_componente
		where ce.cod_deli_estado in (323)
		and ce.dt_deli_novedad_estado > to_date('2020-07-19')
	)a where rnk = 1 group by cod_deli_crupier;
	DROP TABLE IF EXISTS entered_state;
create temporary table entered_state as
	SELECT s.cod_deli_crupier,max(ts_deli_evento) entered
	FROM bi_corp_common.bt_deli_courier_shipping s
	inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
	where cod_deli_estado in (1,2,3,4) and
	to_date(ts_deli_evento) > to_date('2020-07-19')
	GROUP by s.cod_deli_crupier;
	DROP TABLE IF EXISTS dates;
 create temporary table dates as
	select
		to_date(completed) completed,
		days2.working_day entered_expected,
		to_date(entered) entered,
		current_date() today,
		cs.cod_deli_crupier
	FROM completed_state cs
	LEFT JOIN entered_state es on cs.cod_deli_crupier = es.cod_deli_crupier
	LEFT JOIN working_days days1 on to_date(cs.completed) = to_date(days1.working_day)
	LEFT JOIN working_days days2 on days1.ind+1 = days2.ind;
	INSERT overwrite TABLE bi_corp_common.bt_deli_kpi_entregadacorreo
	PARTITION(partition_date)
select distinct
q.cod_deli_crupier,
cod_deli_operacion,
dt_deli_registro,
sp.cod_subproducto,
completed,
entered,
null as delta,
null as delta_sla,
null as delta_hypothetic,
cod_deli_courier_tracking,
cod_deli_codigo,
ds_deli_tipo,
cod_deli_contract,
s.ds_deli_operacion,
s.ds_deli_paquete_crupier,
case when entered <= entered_expected then 1 else 0 end sla,
case when today <= entered_expected then 1 else 0 end sla_hipotetico,
'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
FROM dates q
left join shippings s on q.cod_deli_crupier = s.cod_deli_crupier
left join shippings_subproductos sp on q.cod_deli_crupier = sp.cod_deli_crupier
left join shippings_couriers sc on q.cod_deli_crupier = sc.cod_deli_crupier
left join sequences ss on ss.cod_deli_shipping = sc.cod_deli_shipping
order by cod_deli_crupier desc,cod_deli_courier_tracking desc,cod_deli_codigo desc,ds_deli_tipo desc;