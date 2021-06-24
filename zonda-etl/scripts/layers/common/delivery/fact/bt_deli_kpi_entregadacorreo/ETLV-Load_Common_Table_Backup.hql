set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE pre_holidays as select
        *, rank() over (partition by row_num order BY data_date_part desc) rnk
        from santander_business_risk_arda.calendario where
        flag_feriado = 1 and fec_yyyy_mm_dd <> '2020-11-06' and
        to_date(fec_yyyy_mm_dd) > to_date('2020-07-01') and
        to_date(fec_yyyy_mm_dd) < to_date('2025-01-01');

CREATE TEMPORARY TABLE holidays as
    select row_num,to_date(fec_yyyy_mm_dd) holiday, 1 as join_aux ,CURRENT_DATE as today
    from pre_holidays a
    where rnk = 1;

CREATE TEMPORARY TABLE pre as select
            e.cod_deli_crupier,
            rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
        from bi_corp_common.bt_deli_envios e
        where e.cod_deli_crupier not in
        (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.dt_deli_registro >= to_date('2020-07-19')
        and e.ds_deli_courier is not null;

CREATE TEMPORARY TABLE preshippings as
    select cod_deli_crupier
    from pre
    where rnk = 1;

CREATE TEMPORARY TABLE ship_base as select
            e.cod_deli_crupier,dt_deli_registro,cod_deli_producto,cod_deli_operacion,cod_deli_paquete,ds_deli_operacion,
			ds_deli_paquete_crupier,
            rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) desc) rnk
        from bi_corp_common.bt_deli_envios e
        left join preshippings ps on ps.cod_deli_crupier = e.cod_deli_crupier
        where e.cod_deli_crupier not in
        (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
        and e.dt_deli_registro >= to_date('2020-07-19');

CREATE TEMPORARY TABLE shippings as
    select cod_deli_crupier,
    dt_deli_registro,
    cod_deli_producto,
    cod_deli_operacion,
    ds_deli_operacion,
	ds_deli_paquete_crupier,
    cod_deli_paquete
    from ship_base
    where rnk = 1;

CREATE TEMPORARY TABLE pre_shippings_couriers as
    select
        e.cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking,
        rank() over (partition by cod_deli_shipping order BY to_date(partition_date) desc) rnk
    from bi_corp_common.bt_deli_courier_shipping e
    where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
    and e.ts_deli_creacion > to_date('2020-07-19');

CREATE TEMPORARY TABLE shippings_couriers as
    select cod_deli_crupier,cod_deli_shipping,cod_deli_contract,cod_deli_courier_tracking
    from pre_shippings_couriers where rnk = 1;

CREATE TEMPORARY TABLE pre_shippings_subproductos as
    select
        crupier_id,cod_subproducto,
        rank() over (partition by crupier_id order BY to_date(ultima_modif) desc) rnk
    from bi_corp_staging.rio258_cp_envios e where cod_subproducto is not null and cod_subproducto <> 'null';

CREATE TEMPORARY TABLE shippings_subproductos as
    select cast(crupier_id as int) cod_deli_crupier,cod_subproducto from
    pre_shippings_subproductos
    where rnk = 1;

CREATE TEMPORARY TABLE pre_sequences as
    select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping,
        rank() over (partition by cod_deli_codigo order BY to_date(partition_date) desc) rnk
    from bi_corp_common.bt_deli_courier_component
    where cod_deli_codigo is not null and cod_deli_codigo <> 'null'    --esta linea (omentada en el original)arroja el error 2 en mapred(stage-71) al devolver el string 'null' para cod_deli_codigo
	;

CREATE TEMPORARY TABLE sequences as
    select cod_deli_codigo,ds_deli_tipo,cod_deli_shipping from pre_sequences a where rnk = 1
;

CREATE TEMPORARY TABLE pre_completed_state as
    select
        rank() over (partition by cod_deli_componente_envio order BY to_date(ce.partition_date) desc) rnk,
        dt_deli_novedad_estado,
        c.cod_deli_crupier
    from shippings e
    left join bi_corp_common.bt_deli_componentes c on e.cod_deli_crupier = c.cod_deli_crupier
    left join bi_corp_common.bt_deli_componentes_estados ce on c.cod_deli_componente=ce.cod_deli_componente
    where ce.cod_deli_estado in (323)
    and ce.dt_deli_novedad_estado > to_date('2020-07-19')
    --and    e.cod_deli_crupier not in (select cast(crupier_id as int) from bi_corp_staging.rio258_cp_envios where cod_producto = '90' and cod_subproducto not in ('2001'))
;

CREATE TEMPORARY TABLE completed_state as
    select cod_deli_crupier,max(dt_deli_novedad_estado) as completed from
    pre_completed_state
    where rnk = 1 group by cod_deli_crupier
;

CREATE TEMPORARY TABLE entered_state as
    SELECT s.cod_deli_crupier,max(ts_deli_evento) entered
    FROM bi_corp_common.bt_deli_courier_shipping s
    inner join bi_corp_common.bt_deli_courier_event e on s.cod_deli_shipping=e.cod_deli_shipping
    where cod_deli_estado in (1,2,3,4) and
    to_date(ts_deli_evento) > to_date('2020-07-19')
    GROUP by s.cod_deli_crupier
;

CREATE TEMPORARY TABLE dates as
select
    to_date(entered) entered,
    to_date(completed) completed,
    to_date(date_add(completed,1)) entered_expected,
    cs.cod_deli_crupier,
    1 as join_aux
FROM completed_state cs
LEFT JOIN entered_state es on cs.cod_deli_crupier = es.cod_deli_crupier
;

CREATE TEMPORARY TABLE date_diff as
    select
    cast (datediff(entered,completed) as INT) diff,
    cast (datediff(entered_expected,completed) as INT) diff_sla,
    cast (datediff(CURRENT_DATE,completed) as INT) diff_hypothetic,
    completed,
    entered,
    cod_deli_crupier
    from dates where completed is not null
    ;

CREATE TEMPORARY TABLE date_diff_hol as
    select
        count(DISTINCT case when (h.holiday >= completed and h.holiday <= entered)  then row_num else cast(null as string) end) holidays_diff,
        count(DISTINCT case when (h.holiday >= completed and h.holiday <= entered_expected)  then row_num else cast(null as string) end) holidays_diff_sla,
        count(DISTINCT case when (h.holiday >= completed and h.holiday <= CURRENT_DATE) then row_num else cast(null as string) end) holidays_diff_hypothetic,
        d.cod_deli_crupier
    from holidays h
    left join dates d on h.join_aux = d.join_aux
    where
        (h.holiday >= completed and h.holiday <= entered) or
        (h.holiday >= completed and h.holiday <= entered_expected) or
        (h.holiday >= completed and h.holiday <= h.today)
    group by d.cod_deli_crupier
;
CREATE TEMPORARY TABLE no_holidays_diff as
    select
        dd.completed,
        dd.entered,
        dd.cod_deli_crupier,
        diff - (case when holidays_diff is not null then holidays_diff else 0 end ) as delta,
        diff_sla as delta_sla, -- (case when holidays_diff_sla is not null then holidays_diff_sla else 0 end ) as delta_sla,
        diff_hypothetic - (case when holidays_diff_hypothetic is not null then holidays_diff_hypothetic else 0 end) as delta_hypothetic
    from date_diff dd left join date_diff_hol ddh on dd.cod_deli_crupier = ddh.cod_deli_crupier
;
insert overwrite table bi_corp_common.bt_deli_kpi_entregadacorreo
partition(partition_date)

select distinct
q.cod_deli_crupier,
cod_deli_operacion,
dt_deli_registro,
sp.cod_subproducto,
completed,
entered,
delta,
delta_sla,
delta_hypothetic,
cod_deli_courier_tracking,
cod_deli_codigo,
ds_deli_tipo,
cod_deli_contract,
ds_deli_operacion,
ds_deli_paquete_crupier,
case when delta <= delta_sla then 1 else 0 end sla,
case when delta is null and delta_hypothetic <= delta_sla then 1 else 0 end sla_hypothetic
,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
from no_holidays_diff q
left join shippings s on q.cod_deli_crupier = s.cod_deli_crupier
left join shippings_subproductos sp on q.cod_deli_crupier = sp.cod_deli_crupier
left join shippings_couriers sc on q.cod_deli_crupier = sc.cod_deli_crupier
left join sequences ss on ss.cod_deli_shipping = sc.cod_deli_shipping
where to_date(entered) >= to_date('2020-07-01')
--and sp.cod_subproducto is not null
order by q.cod_deli_crupier desc,sc.cod_deli_courier_tracking desc,cod_deli_codigo desc,ds_deli_tipo desc;