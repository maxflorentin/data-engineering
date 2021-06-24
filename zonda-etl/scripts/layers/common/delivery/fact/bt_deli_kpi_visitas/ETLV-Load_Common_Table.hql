set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;



CREATE TEMPORARY TABLE pre_holidays as select
		*, rank() over (partition by row_num order BY data_date_part desc) rnk
		from santander_business_risk_arda.calendario
		where
		flag_feriado = 1 and fec_yyyy_mm_dd <> '2020-11-06' and
		to_date(fec_yyyy_mm_dd) > to_date('2020-07-01') and
		to_date(fec_yyyy_mm_dd) < to_date('2025-01-01');

CREATE TEMPORARY TABLE holidays as
	select row_num,to_date(fec_yyyy_mm_dd) holiday, 1 as join_aux ,CURRENT_DATE as today
	from pre_holidays a
	where rnk = 1;

CREATE TEMPORARY TABLE pre as
select
		e.cod_deli_crupier,
		rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
		from bi_corp_common.bt_deli_envios e
		where partition_date >= date_sub(current_date,360) and
		e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-01')
		and e.ds_deli_courier is not null;

CREATE TEMPORARY TABLE preshippings as
	select cod_deli_crupier
	from pre
	where rnk = 1;

CREATE TEMPORARY TABLE ship_base as
select
		e.cod_deli_crupier,
		dt_deli_registro,
		cod_deli_producto,
		cod_deli_operacion,
		cod_deli_paquete,
		ds_deli_operacion,
    ds_deli_paquete_crupier,
		cod_deli_codigo_barra,
		rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_envios e
		left join preshippings ps on ps.cod_deli_crupier = e.cod_deli_crupier
		where e.partition_date >= date_sub(current_date,360) and
		e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-01');

CREATE TEMPORARY TABLE shippings as
	select cod_deli_crupier,
	dt_deli_registro,
	cod_deli_producto,
	cod_deli_operacion,
	cod_deli_paquete,
		ds_deli_operacion,
    ds_deli_paquete_crupier,
	cod_deli_codigo_barra
	from ship_base
	where rnk = 1;

CREATE TEMPORARY TABLE pre_courier_shippings as
	select
		e.cod_deli_crupier,e.cod_deli_shipping,
		rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
	from bi_corp_common.bt_deli_courier_shipping e
	where partition_date >= date_sub(current_date,360) and
	e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
	and e.ts_deli_creacion > to_date('2020-07-19');

CREATE TEMPORARY TABLE courier_shippings as
	select cod_deli_crupier,cod_deli_shipping from pre_courier_shippings where rnk = 1;

CREATE TEMPORARY TABLE pre_visit_events_a_2 as
	SELECT e.id,e.internal_shipping_id,e.event_date,
	case when e.status_id in ('63','64') then 'home'
		 when e.status_id in ('65') then 'courier'
		 when e.status_id in ('97') then 'bank'
		 when e.status_id in ('17') then 'destroyed'
		 when e.status_id IN ('162','163','164','165','166','167','168','169','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','341')
		 	then 'failed'
		 else cast(null as string)
	end visit_type,
	rank() over (partition by e.id order BY to_date(e.partition_date) desc) rnk
	from bi_corp_staging.rio258_shipping_event e
	where	e.status_id in ('17','97','63','64','65','162','163','164','165','166','167','168','169','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','341')
	and
	to_date(e.event_date) > to_date('2020-07-19');

CREATE TEMPORARY TABLE pre_visit_events_a_1 as
	select internal_shipping_id,to_date(event_date) visit,visit_type,
			rank() over (partition by internal_shipping_id order BY to_date(event_date) asc) visit_num
		from pre_visit_events_a_2 where rnk = 1;

CREATE TEMPORARY TABLE visit_events_a as
	select distinct * from pre_visit_events_a_1;

CREATE TEMPORARY TABLE pre_visit_events_b_2 as
	SELECT e.id,e.internal_shipping_id,e.event_date,
	case when e.status_id in ('171','172') then 'home'
		 when e.status_id in ('173') then 'courier'
		 when e.status_id in ('97') then 'bank'
		 when e.status_id in ('17') then 'destroyed'
		 when e.status_id IN ('162','163','164','165','166','167','168','169','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','341')
		 then 'failed'
		 else cast(null as string)
	end visit_type,
	rank() over (partition by e.id order BY to_date(e.partition_date) desc) rnk
	from bi_corp_staging.rio258_shipping_event e
	where e.status_id in ('17','97','162','163','164','165','166','167','168','169','171','172','173','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','341')
	and
	to_date(e.event_date) > to_date('2020-07-19');

CREATE TEMPORARY TABLE pre_visit_events_b_1 as
	select internal_shipping_id,to_date(event_date) visit,visit_type,
			rank() over (partition by internal_shipping_id order BY to_date(event_date) asc) visit_num
		from pre_visit_events_b_2 where rnk = 1;

CREATE TEMPORARY TABLE visit_events_b as
	select distinct * from pre_visit_events_b_1;

CREATE TEMPORARY TABLE pre_visit_events as
	select * from
	visit_events_a a
	union all
	select * from
	visit_events_b ;

CREATE TEMPORARY TABLE visit_events as
	select internal_shipping_id,min(visit) visit,visit_type,visit_num
		from pre_visit_events
	group by internal_shipping_id,visit_type,visit_num;

CREATE TEMPORARY TABLE visit_states as
	select distinct s.cod_deli_crupier,visit,visit_type,visit_num
	from courier_shippings s
	inner join visit_events e on cast(s.cod_deli_shipping as string)=e.internal_shipping_id;

CREATE TEMPORARY TABLE entered_state as
	SELECT s.cod_deli_crupier,min(ts_deli_evento) entered
	FROM bi_corp_common.bt_deli_courier_shipping s
	inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
	where 	cod_deli_estado in (1,2,3,4) and
	to_date(ts_deli_evento) > to_date('2020-07-19')
	GROUP by s.cod_deli_crupier;

CREATE TEMPORARY TABLE events as
	select
	    e.cod_deli_crupier,
	    ee.visit as visita,
	    ee.visit_type,
	    ee.visit_num
	from shippings e
	left join visit_states ee on e.cod_deli_crupier=ee.cod_deli_crupier;

CREATE TEMPORARY TABLE dates_bis as
	select
		min(entered) ingresado,
		max(case when visit_num = 1 then to_date(visita) else cast(null as string) end) visita1,
		max(case when visit_num = 2 then to_date(visita) else cast(null as string) end) visita2,
		max(case when visit_num = 3 then to_date(visita) else cast(null as string) end) visita3,
		max(case when visit_type in ('home','courier','bank') then 1 else 0 end) delivered,
		max(case when visit_type in ('destroyed') then 1 else 0 end) destroyed,
		e.cod_deli_crupier
	from entered_state es
	left join events e on es.cod_deli_crupier=e.cod_deli_crupier
	group by e.cod_deli_crupier;

CREATE TEMPORARY TABLE dates as
	select
		to_date(ingresado) ingresado,
		visita1,
		visita2,
		visita3,
		delivered,
		destroyed,
		max(case when ingresado is not null then date_add(ingresado,2) else cast(null as string) end) visita1_expected,
		max(case when ingresado is not null then date_add(ingresado,4) else cast(null as string) end) visita2_expected,
		max(case when ingresado is not null then date_add(ingresado,10) else cast(null as string) end) visita3_expected,
		e.cod_deli_crupier,
		1 as join_aux
	from dates_bis e
	group by
	cod_deli_crupier,
	ingresado,
	visita1,
	visita2,
	visita3,
	delivered,
	destroyed;

CREATE TEMPORARY TABLE date_diff as
	select
	cast (datediff(visita1,ingresado) as INT) diff_1,
	cast (datediff(visita2,ingresado) as INT) diff_2,
	cast (datediff(visita3,ingresado) as INT) diff_3,
	cast (datediff(visita1_expected,ingresado) as INT) diff_1_sla,
	cast (datediff(visita2_expected,ingresado) as INT) diff_2_sla,
	cast (datediff(visita3_expected,ingresado) as INT) diff_3_sla,
	cast (datediff(CURRENT_DATE,ingresado) as INT) diff_hypothetic,
	delivered,
	destroyed,
	cod_deli_crupier
	from dates;

CREATE TEMPORARY TABLE date_diff_hol as
	select
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita1) then row_num else cast(null as string) end) holidays_diff_1,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita2) then row_num else cast(null as string) end) holidays_diff_2,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita3) then row_num else cast(null as string) end) holidays_diff_3,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita1_expected) then row_num else cast(null as string) end) holidays_diff_1_sla,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita2_expected) then row_num else cast(null as string) end) holidays_diff_2_sla,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= visita3_expected) then row_num else cast(null as string) end) holidays_diff_3_sla,
		count(DISTINCT case when (h.holiday >= ingresado and h.holiday <= CURRENT_DATE) then row_num else cast(null as string) end) holidays_diff_hypothetic,
		cod_deli_crupier
	from holidays h
	left join dates d on h.join_aux = d.join_aux
	where
		(h.holiday >= ingresado and h.holiday <= visita1) or
		(h.holiday >= ingresado and h.holiday <= visita2) or
		(h.holiday >= ingresado and h.holiday <= visita3) or
		(h.holiday >= ingresado and h.holiday <= visita1_expected) or
		(h.holiday >= ingresado and h.holiday <= visita2_expected) or
		(h.holiday >= ingresado and h.holiday <= visita3_expected) or
		(h.holiday >= ingresado and h.holiday <= h.today)
	group by cod_deli_crupier;

CREATE TEMPORARY TABLE no_holidays_diff as
	select
		dd.delivered,
		dd.destroyed,
		dd.cod_deli_crupier,
		diff_1 - (case when holidays_diff_1 is not null then holidays_diff_1 else 0 end ) as delta_first_visit,
		diff_1_sla as delta_first_visit_sla,
		diff_2 - (case when holidays_diff_2 is not null then holidays_diff_2 else 0 end ) as delta_second_visit,
		diff_2_sla as delta_second_visit_sla,
		diff_3 - (case when holidays_diff_3 is not null then holidays_diff_3 else 0 end ) as delta_third_visit,
		diff_3_sla as delta_third_visit_sla,
		diff_hypothetic - (case when holidays_diff_hypothetic is not null then holidays_diff_hypothetic else 0 end) as delta_hypothetic
	from date_diff dd left join date_diff_hol ddh on dd.cod_deli_crupier = ddh.cod_deli_crupier;


insert overwrite table bi_corp_common.bt_deli_kpi_visitas
partition(partition_date)

	select
	delta_first_visit,
	(case when delta_first_visit <= delta_first_visit_sla then 1 else 0 end) as sla_first_visit,
	delta_second_visit,
	(case when delta_second_visit <= delta_second_visit_sla then 1 else 0 end) as sla_second_visit,
	delta_third_visit,
	(case when delta_third_visit <= delta_third_visit_sla then 1 else 0 end) as sla_third_visit,
	(case when delivered = 0 and delta_hypothetic < delta_first_visit_sla then 1 else 0 end) as hypothetic_delta_first_visit,
	(case when delivered = 0 and delta_hypothetic < delta_second_visit_sla then 1 else 0 end) as hypothetic_delta_second_visit,
	(case when delivered = 0 and delta_hypothetic < delta_third_visit_sla then 1 else 0 end) as hypothetic_delta_third_visit,
	delivered,
	n.cod_deli_crupier,
	ds_deli_operacion,
    ds_deli_paquete_crupier,
	s.dt_deli_registro
	,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
	from no_holidays_diff n
	inner join shippings s on s.cod_deli_crupier = n.cod_deli_crupier;