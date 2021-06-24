set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set APPX_COUNT_DISTINCT=true;
set mapred.job.queue.name=root.dataeng;



CREATE TEMPORARY TABLE pre as select
			e.cod_deli_crupier, dt_deli_ultima_modificacion,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
		from bi_corp_common.bt_deli_envios e
		where e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-01')
		and e.ds_deli_courier is not null;

CREATE TEMPORARY TABLE preshippings as
	select cod_deli_crupier, dt_deli_ultima_modificacion as fecha_correo
	from pre
	where rnk = 1;

CREATE TEMPORARY TABLE  ship_base as select
			e.cod_deli_crupier,dt_deli_registro,cod_deli_producto,cod_deli_operacion,cod_deli_paquete,cod_deli_codigo_barra,fecha_correo,
				ds_deli_operacion,
    ds_deli_paquete_crupier,
			rank() over (partition by e.cod_deli_crupier order BY to_date(partition_date) asc) rnk
		from bi_corp_common.bt_deli_envios e
		left join preshippings ps on ps.cod_deli_crupier = e.cod_deli_crupier
		where e.cod_deli_crupier not in
		(select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados b)
		and e.dt_deli_registro >= to_date('2020-07-01');

CREATE TEMPORARY TABLE shippings as
	select cod_deli_crupier,
	dt_deli_registro,
	fecha_correo,
	cod_deli_producto,
	cod_deli_operacion,
	cod_deli_paquete,
		ds_deli_operacion,
    ds_deli_paquete_crupier,
	cod_deli_codigo_barra
	from ship_base
	where rnk = 1;


CREATE TEMPORARY TABLE pre_fecha_evento as
select e.cod_deli_shipping,
to_date(e.ts_deli_evento) ts_deli_evento,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario,
rank() over (partition by e.cod_deli_shipping, e.cod_deli_estado order BY to_date(e.ts_deli_evento) desc) rnk
from bi_corp_common.bt_deli_courier_event e;

CREATE TEMPORARY TABLE min_fecha_evento as
select e.cod_deli_shipping,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario,
max(to_date(e.ts_deli_evento)) ts_deli_evento
from pre_fecha_evento e
where e.rnk = 1
group by e.cod_deli_shipping,
e.cod_deli_estado,
e.ds_deli_estado,
e.ds_deli_motivo,
e.ds_deli_motivo_secundario;


CREATE TEMPORARY TABLE entregado_sucursal as
select distinct s.cod_deli_crupier, e.cod_deli_estado, e.ds_deli_estado
from shippings s
inner join bi_corp_common.bt_deli_courier_shipping cs on cs.cod_deli_crupier = s.cod_deli_crupier
inner join min_fecha_evento e on cs.cod_deli_shipping = e.cod_deli_shipping
where cs.flag_deli_cancelado = 0
and cod_deli_estado  in (63,64,65,17,97,171,172,173,382,401,151,155,160,257);

CREATE TEMPORARY TABLE pre_ultimo_estado as
select 'c_estados' as kpi,
s.cod_deli_crupier,
cod_deli_operacion,
cod_deli_codigo_barra,
dt_deli_registro,
fecha_correo,
ds_deli_estado,
ds_deli_operacion,
ds_deli_paquete_crupier,
rank() over (partition by s.cod_deli_crupier order BY to_date(e.ts_deli_evento) desc) rnk,
ts_deli_evento
from shippings s
inner join bi_corp_common.bt_deli_courier_shipping cs on cs.cod_deli_crupier = s.cod_deli_crupier
inner join min_fecha_evento e on cs.cod_deli_shipping = e.cod_deli_shipping
where cs.flag_deli_cancelado = 0;

CREATE TEMPORARY TABLE ultimos_estados as
select *
from pre_ultimo_estado
where rnk = 1;

insert overwrite table bi_corp_common.bt_deli_kpi_estadototal
partition(partition_date)

select distinct
s.cod_deli_crupier,
cod_deli_operacion,
cod_deli_codigo_barra,
dt_deli_registro,
s.fecha_correo as fecha_actualizacion,
s.ds_deli_estado,
ds_deli_operacion,
ds_deli_paquete_crupier,
s.ts_deli_evento
,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
from ultimos_estados s
inner join bi_corp_common.bt_deli_courier_shipping cs on cs.cod_deli_crupier = s.cod_deli_crupier
inner join min_fecha_evento e on cs.cod_deli_shipping = e.cod_deli_shipping