set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;


DROP TABLE IF EXISTS shippings;
CREATE TEMPORARY TABLE shippings as
	select a.cod_deli_crupier,dt_deli_registro ,cod_deli_producto,ds_deli_operacion,ds_deli_paquete_crupier from (
		select
			e.cod_deli_crupier,dt_deli_registro,cod_deli_producto,ds_deli_operacion,ds_deli_paquete_crupier,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
	from bi_corp_common.bt_deli_envios e
		where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro > to_date('2020-07-19')
	)a where rnk = 1
;DROP TABLE IF EXISTS courier_shippings;
CREATE TEMPORARY TABLE courier_shippings as
	select a.cod_deli_crupier,cod_deli_shipping,cod_deli_courier_tracking,cod_deli_postal from (
		select
			e.cod_deli_crupier,cod_deli_shipping,cod_deli_courier_tracking,cod_deli_postal,
			rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
		from bi_corp_common.bt_deli_courier_shipping e
		where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.ts_deli_creacion > to_date('2020-07-19')
	)a where rnk = 1
;DROP TABLE IF EXISTS failed_visit_events;
CREATE TEMPORARY TABLE failed_visit_events as
	select distinct * from (
		select
			e.cod_deli_shipping,
			e.cod_deli_estado,
			e.ds_deli_estado,
			e.ds_deli_motivo,
			e.ds_deli_motivo_secundario,
			e.ts_deli_evento ts_deli_evento,
			rank() over (partition by cod_deli_shipping order BY ts_deli_evento asc) visit_num
		from (
			select
				e.cod_deli_shipping,
				to_date(e.ts_deli_evento) ts_deli_evento,
				e.cod_deli_estado,
				e.ds_deli_estado,
				e.ds_deli_motivo,
				e.ds_deli_motivo_secundario,
				rank() over (partition by cod_deli_courier_event order BY to_date(e.partition_date) asc) rnk
				from bi_corp_common.bt_deli_courier_event e
				where cod_deli_estado in (229,341,162,163,164,165,166,167,168,169,174,175
				,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195
				,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215
				,216,217,218,219,220,221,222,223,224,225,226,227,228)
				and to_date(e.ts_deli_evento) > to_date('2020-07-19')
		) e where e.rnk = 1
	) b order by cod_deli_shipping asc,visit_num desc
;DROP TABLE IF EXISTS failed_visit_states;
CREATE TEMPORARY TABLE failed_visit_states as
	select distinct
	cod_deli_crupier,
	cod_deli_postal,
	cod_deli_courier_tracking,
	ts_deli_evento,
	cod_deli_estado,
	ds_deli_estado,
	ds_deli_motivo,
	ds_deli_motivo_secundario
	from courier_shippings s
	inner join failed_visit_events e on s.cod_deli_shipping=e.cod_deli_shipping
;DROP TABLE IF EXISTS filter_states;
CREATE TEMPORARY TABLE filter_states as
    SELECT s.cod_deli_crupier,max(ts_deli_evento)  event
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (19,17,13,14,15,63,64,65,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,
    116,117,118,119,120,121,122,123,124,157,158,159,300,301,302,173,172,171,97,401,31) and
    to_date(ts_deli_evento) > to_date('2020-07-19') and
    s.flag_deli_cancelado = 0
    GROUP by cod_deli_crupier
;DROP TABLE IF EXISTS custody_states;
CREATE TEMPORARY TABLE custody_states as
    SELECT s.cod_deli_crupier,s.cod_deli_shipping,max(ts_deli_evento)  eventcus
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (5,16) and
    to_date(ts_deli_evento) > to_date('2020-07-19') and
    s.flag_deli_cancelado = 0
    GROUP by cod_deli_crupier,s.cod_deli_shipping;


insert overwrite table bi_corp_common.bt_deli_kpi_enviofallido
partition(partition_date)

select distinct
 fvs.cod_deli_crupier,
	fvs.ts_deli_evento,
	fsc.eventcus,
	e.cod_deli_producto,
	fvs.cod_deli_courier_tracking,
	fsc.cod_deli_shipping,
	fvs.cod_deli_estado,
	fvs.ds_deli_estado,
	fvs.ds_deli_motivo,
	fvs.ds_deli_motivo_secundario,
	e.ds_deli_operacion,
	e.ds_deli_paquete_crupier,
	fvs.ts_deli_evento
,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
from shippings e
left join failed_visit_states fvs on e.cod_deli_crupier=fvs.cod_deli_crupier
left join filter_states fs on fvs.cod_deli_crupier=fs.cod_deli_crupier
left join custody_states fsc on fvs.cod_deli_crupier=fsc.cod_deli_crupier
where event is null