set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;
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
    select a.cod_deli_crupier,a.dt_deli_registro,a.cod_deli_producto,a.cod_deli_operacion,a.cod_deli_paquete,a.cod_deli_codigo_barra,a.ds_deli_paquete_crupier
    from (
        select
            e.cod_deli_crupier,e.dt_deli_registro,e.cod_deli_producto,e.cod_deli_operacion,e.cod_deli_paquete,e.cod_deli_codigo_barra,e.ds_deli_paquete_crupier,
            rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
        from bi_corp_common.bt_deli_envios e
        where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.dt_deli_registro > to_date('2020-07-19')
    )a where rnk = 1;
DROP TABLE IF EXISTS courier_shippings;
create temporary table courier_shippings as
    select a.cod_deli_crupier,a.cod_deli_shipping,a.cod_deli_codigo_barra from (
        select
            e.cod_deli_crupier,cod_deli_shipping,cod_deli_courier_tracking as cod_deli_codigo_barra,
            rank() over (partition by e.cod_deli_shipping order BY to_date(partition_date) desc) rnk
        from bi_corp_common.bt_deli_courier_shipping e
        where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.ts_deli_creacion > to_date('2020-07-19')
    )a where rnk = 1;
DROP TABLE IF EXISTS custody_state;
create temporary table custody_state as
    SELECT s.cod_deli_crupier,min(ts_deli_evento) custody
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (16,5) and
    to_date(ts_deli_evento) > to_date('2020-07-19')
    GROUP by cod_deli_crupier;
DROP TABLE IF EXISTS destroyed_state;
create temporary table destroyed_state as
    SELECT s.cod_deli_crupier,max(ts_deli_evento) filters
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (14,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,
    59,60,61,62,17,155,156,157,158,159,160,63,64,65,97,151,171,172,173,257,17) and
    to_date(ts_deli_evento) > to_date('2020-07-19')
    GROUP by cod_deli_crupier;
DROP TABLE IF EXISTS dates;
create temporary table dates as
    select
    to_date(custody) custody,
    days2.working_day destroyed,
	to_date(current_date()) today,
    cs.cod_deli_crupier
    FROM custody_state cs
    LEFT JOIN destroyed_state ds on cs.cod_deli_crupier = ds.cod_deli_crupier
	LEFT JOIN working_days days1 on to_date(custody) = to_date(days1.working_day)
	LEFT JOIN working_days days2 on days1.ind+30 = days2.ind
    where filters is null;
    insert overwrite table bi_corp_common.bt_deli_kpi_destruidos
    partition(partition_date)
    select
    q.cod_deli_crupier,
    cod_deli_producto,
    cod_deli_operacion,
    cod_deli_paquete,
	s.cod_deli_codigo_barra,
    custody,
    q.destroyed,
	null as typecard,
	null as delta,
	null as delta_sla,
	null as delta_hypothetic,
	null as sla,
	null as sla_hypothetic,
    null as ds_deli_operacion,
	ds_deli_paquete_crupier,
	dt_deli_registro,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
	from dates q
    left join shippings s on q.cod_deli_crupier = s.cod_deli_crupier
    left join courier_shippings cs on q.cod_deli_crupier = cs.cod_deli_crupier
    WHERE destroyed < today
    order by custody desc;