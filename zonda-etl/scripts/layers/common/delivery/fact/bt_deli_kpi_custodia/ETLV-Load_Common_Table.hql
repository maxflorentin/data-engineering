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
    select a.cod_deli_crupier,a.dt_deli_registro,a.cod_deli_producto,a.cod_deli_operacion,a.cod_deli_paquete,a.ds_deli_operacion,
			a.ds_deli_paquete_crupier
    from (
        select
            e.cod_deli_crupier,e.dt_deli_registro,e.cod_deli_producto,e.cod_deli_operacion,e.cod_deli_paquete,e.ds_deli_operacion,e.ds_deli_paquete_crupier,
            rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
        from bi_corp_common.bt_deli_envios e
        where e.cod_deli_crupier not in
        (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.dt_deli_registro > to_date('2020-07-19')
    )a where rnk = 1;
DROP TABLE IF EXISTS courier_shippings;
create temporary table courier_shippings as
    select a.cod_deli_crupier,cod_deli_shipping,cod_deli_courier_tracking as cod_deli_codigo_barra from (
        select
            e.cod_deli_crupier,cod_deli_shipping,cod_deli_courier_tracking,
            rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
        from bi_corp_common.bt_deli_courier_shipping e
        where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.ts_deli_creacion > to_date('2020-07-19')
    )a where rnk = 1;
    DROP TABLE IF EXISTS entered_state;
    create temporary table entered_state as
    SELECT s.cod_deli_crupier,min(ts_deli_evento) entered
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (1,2,3,4) and
    to_date(ts_deli_evento) > to_date('2020-07-19') and
    s.flag_deli_cancelado = 0
    GROUP by s.cod_deli_crupier;

DROP TABLE IF EXISTS filter_states;
create temporary table filter_states as
    SELECT s.cod_deli_crupier,max(ts_deli_evento)  event--,min(ts_deli_evento) delivered
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (16,5,97,61,62,63,152,19,13,14,15,155,156,157,158,159,160) and
    to_date(ts_deli_evento) > to_date('2020-07-19') and
    s.flag_deli_cancelado = 0
    GROUP by s.cod_deli_crupier;
    DROP TABLE IF EXISTS dates;
    create temporary table dates as
    select
        to_date(entered) entered,
        days2.working_day custody,
        to_date(current_date()) today,
        es.cod_deli_crupier
    FROM entered_state es
    left JOIN filter_states cs on es.cod_deli_crupier = cs.cod_deli_crupier
	LEFT JOIN working_days days1 on to_date(entered) = to_date(days1.working_day)
	LEFT JOIN working_days days2 on days1.ind+10 = days2.ind
    where event IS NULL;
    insert overwrite table bi_corp_common.bt_deli_kpi_custodia
    partition(partition_date)
select  distinct
    q.cod_deli_crupier,
	s.cod_deli_producto,
    s.cod_deli_operacion,
    s.cod_deli_paquete,
    cod_deli_codigo_barra,
    custody,
    entered,
	null as delta,
	null as delta_sla,
	null as sla,
	s.ds_deli_operacion,
	s.ds_deli_paquete_crupier,
	s.dt_deli_registro,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
		from dates q
    left join shippings s on q.cod_deli_crupier = s.cod_deli_crupier
    left join courier_shippings cs on q.cod_deli_crupier = cs.cod_deli_crupier
    WHERE
    custody < today
    order by entered desc;