set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;
DROP TABLE IF EXISTS  max_part;
create temporary table max_part as
	select max(data_date_part) as  data_date_part  from santander_business_risk_arda.calendario
	where data_date_part > date_sub(CURRENT_DATE(), -7);

DROP TABLE IF EXISTS  working_days;
create temporary table working_days as
	select
		row_number() over (order by row_num) ind,
		row_num,
		1 as join_aux,
		to_date(fec_yyyy_mm_dd) working_day,
		current_date() as today,
		a.data_date_part
	from (
		select
		*, rank() over (partition by row_num order BY data_date_part desc) rnk
		from santander_business_risk_arda.calendario where
		to_date(fec_yyyy_mm_dd) >= to_date('2020-01-01') and
		to_date(fec_yyyy_mm_dd) <= to_date('2025-01-01')
	) a
	inner join max_part mp on a.data_date_part = mp.data_date_part
	where rnk = 1 and flag_laborable = 1 and fec_yyyy_mm_dd not like '%-11-06';

DROP TABLE IF EXISTS  pre;
create temporary table pre as
	select
	e.cod_deli_crupier,
	rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
	from bi_corp_common.bt_deli_envios e
	where e.cod_deli_crupier not in
	(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
	and e.dt_deli_registro >= to_date('2020-07-19')
	and e.ds_deli_courier is not null;

DROP TABLE IF EXISTS  preshippings;
	create temporary table preshippings as
	select cod_deli_crupier
	from pre
	where rnk = 1;

DROP TABLE IF EXISTS  ship_base;
create temporary table ship_base as
		select
			e.cod_deli_crupier,dt_deli_registro,cod_deli_producto,cod_deli_operacion,cod_deli_paquete,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_envios e
		left join preshippings ps on ps.cod_deli_crupier = e.cod_deli_crupier
		where e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-19');

DROP TABLE IF EXISTS  shippings;
create temporary table shippings as
	select cod_deli_crupier,
	dt_deli_registro,
	cod_deli_producto,
	cod_deli_operacion,
	cod_deli_paquete
	from ship_base
	where rnk = 1;

DROP TABLE IF EXISTS  pre_shippings_couriers;
create temporary table pre_shippings_couriers as
	select
		e.cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking,
		rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
	from bi_corp_common.bt_deli_courier_shipping e
	where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
	and e.ts_deli_creacion > to_date('2020-07-19');

DROP TABLE IF EXISTS  shippings_couriers;
create temporary table shippings_couriers as
	select cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking
	from pre_shippings_couriers where rnk = 1;

DROP TABLE IF EXISTS  pre_shippings_subproductos;
create temporary table pre_shippings_subproductos as
	select
		crupier_id,cod_subproducto,
		rank() over (partition by crupier_id order BY to_date(ultima_modif) desc) rnk
	from bi_corp_staging.rio258_cp_envios e where cod_subproducto is not null and cod_subproducto <> 'null';

DROP TABLE IF EXISTS  shippings_subproductos;
create temporary table shippings_subproductos as
	select cast(crupier_id as int) cod_deli_crupier,cod_subproducto from
	pre_shippings_subproductos
	where rnk = 1;

DROP TABLE IF EXISTS  pre_sequences;
create temporary table pre_sequences as
	select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping,
		rank() over (partition by cod_deli_codigo order BY to_date(partition_date) desc) rnk
	from bi_corp_common.bt_deli_courier_component;

DROP TABLE IF EXISTS  sequences;
create temporary table sequences as
	select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping from pre_sequences a where rnk = 1 ;

DROP TABLE IF EXISTS  pre_completed_state;
create temporary table pre_completed_state as
	select
		rank() over (partition by cod_deli_componente_envio order BY to_date(ce.partition_date) desc) rnk,
	    dt_deli_novedad_estado,
	    c.cod_deli_componente,
	    c.cod_deli_crupier,
	    c.cod_deli_producto_tarjeta
	from shippings e
	left join bi_corp_common.bt_deli_componentes c on e.cod_deli_crupier = c.cod_deli_crupier
	left join bi_corp_common.bt_deli_componentes_estados ce on c.cod_deli_componente=ce.cod_deli_componente
	where ce.cod_deli_estado in (323)
	and ce.dt_deli_novedad_estado > to_date('2020-07-19');

DROP TABLE IF EXISTS  completed_state;
create temporary table completed_state as
	select cod_deli_componente,cod_deli_crupier, cod_deli_producto_tarjeta, max(dt_deli_novedad_estado) as completed from
	pre_completed_state
	where rnk = 1 group by cod_deli_componente,cod_deli_crupier,cod_deli_producto_tarjeta;

DROP TABLE IF EXISTS  pre_accepted_state;
create temporary table pre_accepted_state as
	select
		rank() over (partition by cod_deli_componente_envio order BY to_date(ce.partition_date) desc) rnk,
	    dt_deli_novedad_estado,
	    c.cod_deli_componente,
	    c.cod_deli_crupier,
	    c.cod_deli_producto_tarjeta
	from shippings e
	left join bi_corp_common.bt_deli_componentes c on e.cod_deli_crupier = c.cod_deli_crupier
	left join bi_corp_common.bt_deli_componentes_estados ce on c.cod_deli_componente=ce.cod_deli_componente
	where ce.cod_deli_estado in (201)
	and ce.dt_deli_novedad_estado > to_date('2020-07-19');

DROP TABLE IF EXISTS  accepted_state;
create temporary table accepted_state as
	select cod_deli_componente,cod_deli_crupier,cod_deli_producto_tarjeta, max(dt_deli_novedad_estado) as accepted from
	pre_accepted_state
	where rnk = 1 group by cod_deli_componente,cod_deli_crupier,cod_deli_producto_tarjeta;

DROP TABLE IF EXISTS  dates;
create temporary table dates as
select
	to_date(accepted) accepted,
	to_date(completed) completed,
	(
		case
		when cs.cod_deli_producto_tarjeta in (8,15) and s.cod_deli_operacion in (1,2,6) then to_date(date_add(accepted,1))
		when s.cod_deli_operacion in (1,2,6) then to_date(date_add(accepted,3))
		when s.cod_deli_operacion in (3,4) then to_date(date_add(accepted,1))
		else to_date(date_add(accepted,3))  -- default
		end
	) accepted_expected,
	cs.cod_deli_componente,
	cs.cod_deli_crupier,
	cs.cod_deli_producto_tarjeta,
	s.cod_deli_operacion,
	1 as join_aux
FROM completed_state cs
LEFT JOIN accepted_state es on cs.cod_deli_componente = es.cod_deli_componente
LEFT JOIN shippings s on cs.cod_deli_crupier = s.cod_deli_crupier;

DROP TABLE IF EXISTS  date_diff;
create temporary table date_diff as
	select
	cast (datediff(accepted,completed) as INT) diff,
	cast (datediff(accepted_expected,completed) as INT) diff_sla,
	cast (datediff(CURRENT_DATE,completed) as INT) diff_hypothetic,
	completed,
	accepted,
	cod_deli_crupier,
	cod_deli_componente,
	cod_deli_operacion,
	cod_deli_producto_tarjeta
	from dates where completed is not null;

DROP TABLE IF EXISTS  date_diff_hol;
create temporary table date_diff_hol as
	select
		count(DISTINCT case when (h.working_day >= completed and h.working_day <= accepted)  then row_num else cast(null as string) end) holidays_diff,
		count(DISTINCT case when (h.working_day >= completed and h.working_day <= accepted_expected)  then row_num else cast(null as string) end) holidays_diff_sla,
		count(DISTINCT case when (h.working_day >= completed and h.working_day <= CURRENT_DATE) then row_num else cast(null as string) end) holidays_diff_hypothetic,
		d.cod_deli_componente
	from working_days h
	left join dates d on h.join_aux = d.join_aux
	where
		(h.working_day >= completed and h.working_day <= accepted) or
		(h.working_day >= completed and h.working_day <= accepted_expected) or
		(h.working_day >= completed and h.working_day <= h.today)
	group by d.cod_deli_componente;

DROP TABLE IF EXISTS  no_holidays_diff;
create temporary table no_holidays_diff as
	select
		dd.completed,
		dd.accepted,
		dd.cod_deli_crupier,
		dd.cod_deli_componente,
		dd.cod_deli_operacion,
		dd.cod_deli_producto_tarjeta,
		diff - (case when holidays_diff is not null then holidays_diff else 0 end ) as delta,
		diff_sla as delta_sla,
		diff_hypothetic - (case when holidays_diff_hypothetic is not null then holidays_diff_hypothetic else 0 end) as delta_hypothetic
	from date_diff dd
	left join date_diff_hol ddh on dd.cod_deli_componente = ddh.cod_deli_componente;

    insert overwrite table bi_corp_common.bt_deli_kpi_entregadaprisma
    partition(partition_date)
    select distinct
    q.cod_deli_crupier,
    q.cod_deli_operacion,
    q.cod_deli_producto_tarjeta,
    sp.cod_subproducto,
    completed,
    accepted,
    delta,
    delta_sla,
    delta_hypothetic,
    cod_deli_courier_tracking,
    cod_deli_codigo,
    ds_deli_tipo,
    cod_deli_contract,
    case when delta <= delta_sla then 1 else 0 end sla,
    case when delta is null and delta_hypothetic <= delta_sla then 1 else 0 end sla_hypothetic,
    dt_deli_registro,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
	from no_holidays_diff q
    left join shippings s on q.cod_deli_crupier = s.cod_deli_crupier
    left join shippings_subproductos sp on q.cod_deli_crupier = sp.cod_deli_crupier
    left join shippings_couriers sc on q.cod_deli_crupier = sc.cod_deli_crupier
    left join sequences ss on ss.cod_deli_shipping = sc.cod_deli_shipping
    order by q.cod_deli_crupier desc,sc.cod_deli_courier_tracking desc,cod_deli_codigo desc,ds_deli_tipo desc;