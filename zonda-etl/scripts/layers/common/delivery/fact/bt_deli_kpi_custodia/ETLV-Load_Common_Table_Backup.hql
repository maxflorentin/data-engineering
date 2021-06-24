set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;

create temporary table pre_holidays as
select
		*, rank() over (partition by row_num order BY data_date_part desc) rnk
		from santander_business_risk_arda.calendario where
		flag_feriado = 1 and
		to_date(fec_yyyy_mm_dd) > to_date('2020-07-01') and
		to_date(fec_yyyy_mm_dd) < to_date('2025-01-01');

create temporary table holidays as
	select row_num,to_date(fec_yyyy_mm_dd) holiday, 1 as join_aux
	from pre_holidays a
	where rnk = 1;

create temporary table pre as
select
	e.cod_deli_crupier, dt_deli_ultima_modificacion,
	rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
	from bi_corp_common.bt_deli_envios e
	where partition_date >= date_sub(current_date,360) and
	e.cod_deli_crupier not in
	(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
	and e.dt_deli_registro >= to_date('2020-07-01')
	and e.ds_deli_courier is not null;

create temporary table preshippings as
	select cod_deli_crupier, dt_deli_ultima_modificacion as fecha_correo
	from pre
	where rnk = 1;

create temporary table ship_base as
select
			e.cod_deli_crupier,
			dt_deli_registro,
			cod_deli_producto,
			cod_deli_operacion,
			cod_deli_paquete,
			cod_deli_codigo_barra,
			fecha_correo,
			ds_deli_operacion,
			ds_deli_paquete_crupier,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_envios e
		left join preshippings ps on ps.cod_deli_crupier = e.cod_deli_crupier
		where e.partition_date >= date_sub(current_date,360) and
		e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-01');

create temporary table shippings as
	select cod_deli_crupier,
	dt_deli_registro,
	fecha_correo,
	cod_deli_producto,
	cod_deli_operacion,
	cod_deli_paquete,
	cod_deli_codigo_barra,
	ds_deli_operacion,
	ds_deli_paquete_crupier
	from ship_base
	where rnk = 1;
create temporary table pre_shippings_couriers as
    select
        e.cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking as cod_deli_codigo_barra,flag_deli_cancelado,
        rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
    from bi_corp_common.bt_deli_courier_shipping e
    where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
    and e.ts_deli_creacion > to_date('2020-07-19')
;
create temporary table shippings_couriers as
    select cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_codigo_barra,flag_deli_cancelado
    from pre_shippings_couriers where rnk = 1
;

create temporary table pre_min_fecha as
	select e.cod_deli_shipping,
	to_date(e.ts_deli_evento) ts_deli_evento,
	e.cod_deli_estado,
	e.ds_deli_estado,
	e.ds_deli_motivo,
	e.ds_deli_motivo_secundario,
	rank() over (partition by e.cod_deli_shipping, e.cod_deli_estado order BY to_date(e.ts_deli_evento) asc) rnk
from bi_corp_common.bt_deli_courier_event e;

create temporary table min_fecha_evento as
select  e.cod_deli_shipping,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario,
min(to_date(e.ts_deli_evento)) ts_deli_evento
from pre_min_fecha e
where e.rnk = 1
group by e.cod_deli_shipping,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario);

create temporary table shippings_states as
select s.cod_deli_crupier,
dt_deli_registro,
cod_deli_producto,
cod_deli_operacion,
cod_deli_paquete,
cs.cod_deli_codigo_barra,
e.cod_deli_shipping,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario,
ds_deli_operacion,
ds_deli_paquete_crupier,
ts_deli_evento
from shippings s
inner join shippings_couriers cs on cs.cod_deli_crupier = s.cod_deli_crupier
inner join min_fecha_evento e on cs.cod_deli_shipping = e.cod_deli_shipping
where cs.flag_deli_cancelado = 0;



create temporary table entered_state as

	SELECT  s.cod_deli_crupier,
	max(to_date(ts_deli_evento)) entered,
	ds_deli_estado as estado_entrado
	FROM bi_corp_common.bt_deli_courier_shipping s
	inner join min_fecha_evento e on s.cod_deli_shipping=e.cod_deli_shipping
	where s.partition_date >= date_sub(current_date,360) and
	cod_deli_estado in (1,2,3,4)
	and to_date(ts_deli_evento) > to_date('2020-07-01') and
	s.flag_deli_cancelado = 0
	group by s.cod_deli_crupier,ds_deli_estado;



create temporary table delivered_state as
SELECT s.cod_deli_crupier,min(to_date(ts_deli_evento)) delivered, ds_deli_estado as estado_entregado
	FROM bi_corp_common.bt_deli_courier_shipping s
	inner join min_fecha_evento e on s.cod_deli_shipping=e.cod_deli_shipping
	where s.partition_date >= date_sub(current_date,360) and
	cod_deli_estado  in (97,63,64,65) and
	to_date(ts_deli_evento) > to_date('2020-07-01') and
	s.flag_deli_cancelado = 0
	GROUP by cod_deli_crupier, ds_deli_estado;


create temporary table custody_state as
SELECT s.cod_deli_crupier,ds_deli_estado as estado_custodia,min(to_date(ts_deli_evento)) custody
	FROM bi_corp_common.bt_deli_courier_shipping s
	inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
	where s.partition_date >= date_sub(current_date,360) and
	cod_deli_estado = 16
	and
	to_date(ts_deli_evento) > to_date('2020-07-01')
	GROUP by cod_deli_crupier,ds_deli_estado;

create temporary table dates as
	select
		to_date(entered) entered,
		to_date(custody) custody,
		to_date(date_add(entered,10)) custody_expected,
		es.cod_deli_crupier,
		1 as join_aux
	FROM entered_state es
	LEFT JOIN custody_state cs on es.cod_deli_crupier = cs.cod_deli_crupier
	left join delivered_state ds on es.cod_deli_crupier = ds.cod_deli_crupier
	where delivered is null
	and custody is not null;


create temporary table date_diff as
	select
	cast (datediff(custody,entered) as INT) diff,
	cast (datediff(custody_expected,entered) as INT) diff_sla,
	custody,
	entered,
	cod_deli_crupier
	from dates where entered is not null;

create temporary table date_diff_hol as
	select
		count(DISTINCT case when (h.holiday >= d.entered and h.holiday <= d.custody)  then row_num else cast(null as string) end) holidays_diff,
		count(DISTINCT case when (h.holiday >= d.entered and h.holiday <= d.custody_expected)  then row_num else cast(null as string) end) holidays_diff_sla,
		cod_deli_crupier
	from holidays h
	left join dates d on h.join_aux = d.join_aux

	where (h.holiday >= d.entered and h.holiday <= d.custody)
	or (h.holiday >= d.entered and h.holiday <= d.custody_expected)
	group by cod_deli_crupier;

create temporary table no_holidays_diff as
	select
		dd.entered,
		dd.custody,
		dd.cod_deli_crupier,
		diff - (case when holidays_diff is not null then holidays_diff else 0 end ) as delta,
		diff_sla as delta_sla
	from date_diff dd
	left join date_diff_hol ddh on dd.cod_deli_crupier = ddh.cod_deli_crupier;


insert overwrite table bi_corp_common.bt_deli_kpi_custodia
partition(partition_date)


select
	q.cod_deli_crupier,
	cod_deli_producto,
	cod_deli_operacion,
	cod_deli_paquete,
	cod_deli_codigo_barra,
	custody,
	entered,
	delta,
	delta_sla,
	case when delta <= delta_sla then 1 else 0 end sla,
	ds_deli_operacion,
    ds_deli_paquete_crupier,
	dt_deli_registro
	,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
		from no_holidays_diff q
	left join shippings_states s on q.cod_deli_crupier = s.cod_deli_crupier