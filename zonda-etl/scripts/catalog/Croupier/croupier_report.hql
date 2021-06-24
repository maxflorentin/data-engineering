"set mapred.job.queue.name=root.dataeng;
with shippings as (
select a.cod_deli_crupier,
a.cod_per_nup,
a.cod_deli_operacion,
a.ds_deli_operacion,
a.cod_deli_producto,
a.ds_deli_paquete_crupier,
a.cod_deli_canal_venta,
a.ds_deli_canal_venta,
a.cod_deli_ejecutivo_comercial,
a.cod_suc_sucursal from (
		select
			e.cod_deli_crupier,
			e.cod_per_nup,
			e.cod_deli_operacion,
			e.ds_deli_operacion,
			e.dt_deli_registro,
			e.cod_deli_producto,
			e.ds_deli_paquete_crupier,
			e.cod_deli_canal_venta,
	        e.ds_deli_canal_venta,
	        e.cod_deli_ejecutivo_comercial,
	        e.cod_suc_sucursal,
			e.cod_deli_paquete,
			e.cod_deli_codigo_barra,
			rank() over (partition by e.cod_deli_crupier order BY to_date(e.partition_date) desc) rnk
		from bi_corp_common.bt_deli_envios as e
		where e.cod_deli_crupier not in (select b.cod_deli_crupier from bi_corp_common.bt_deli_envios_eliminados as b)
		and TO_DATE(e.dt_deli_registro) > '2020-07-19'
	) as a where rnk = 1
)
select 	cod_deli_crupier,
	    cod_per_nup,
	    cod_deli_operacion,
	    ds_deli_operacion,
		to_date(max(ts_deli_estado)),
	    cod_deli_estado,
	    ds_deli_estado_crupier_env,
	    cod_deli_producto,
	    ds_deli_paquete_crupier,
	    cod_deli_canal_venta,
	    ds_deli_canal_venta,
	    cod_deli_ejecutivo_comercial,
	    cod_suc_sucursal
	    from (
	select
		rank() over (partition by cod_deli_estado_envio order BY to_date(ee.partition_date) desc) rnk,
	    e.cod_deli_crupier,
	    e.cod_per_nup,
	    e.cod_deli_operacion,
	    e.ds_deli_operacion,
	    ee.ts_deli_estado,
	    ee.cod_deli_estado,
	    eE.ds_deli_estado_crupier_env,
	    e.cod_deli_producto,
	    e.ds_deli_paquete_crupier,
	    e.cod_deli_canal_venta,
	    e.ds_deli_canal_venta,
	    e.cod_deli_ejecutivo_comercial,
	    e.cod_suc_sucursal
	from shippings as e
	left join bi_corp_common.bt_deli_envios_estados as ee on e.cod_deli_crupier = ee.cod_deli_crupier
	where ee.cod_deli_estado in (90016,90011,90504,90505)
	and TO_DATE(ee.ts_deli_estado) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='CROUPIER_REPORT') }}'
) as a where rnk = 1
group by cod_deli_crupier,
			  cod_deli_estado,
			  ds_deli_estado_crupier_env,
			  cod_per_nup,
			  cod_deli_operacion,
			  ds_deli_operacion,
   			  cod_deli_producto,
	    		  ds_deli_paquete_crupier,
        		  Cod_deli_caNal_venta,
	              ds_deli_canal_venta,
	    		  cod_deli_ejecutivo_comercial,
	    		  cod_suc_sucursal
"