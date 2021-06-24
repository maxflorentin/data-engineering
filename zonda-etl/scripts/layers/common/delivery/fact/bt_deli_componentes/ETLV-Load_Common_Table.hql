set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_deli_componentes
partition(partition_date)
SELECT
         Cp_Envios_Comp.COD_COMPONENTE as cod_deli_componente
        ,Cp_Envios_Comp.CRUPIER_ID as cod_deli_crupier
        ,Cp_Clientes.cod_per_nup
        ,trim(Cp_Envios_Comp.COD_PRODUCTO) as cod_deli_producto_componente
        ,Cp_Productos_Crupier.ds_deli_producto_componente
        ,case when Cp_Envios_Comp.NRO_TARJETA='null' then null else Cp_Envios_Comp.NRO_TARJETA end as cod_deli_nro_tarjeta_componente
        ,case when Cp_Envios_Comp.NRO_NOVEDAD_AMAS='null' then null else Cp_Envios_Comp.NRO_NOVEDAD_AMAS end as cod_deli_nro_novedad_amas
        ,case when Cp_Envios_Comp.NRO_SECUENCIA_AMAS='null' then null else Cp_Envios_Comp.NRO_SECUENCIA_AMAS end as cod_deli_nro_secuencia_amas
        ,case when trim(Cp_Envios_Comp.COD_PRODUCTO_TARJETA)='null' then null else trim(Cp_Envios_Comp.COD_PRODUCTO_TARJETA) end as cod_deli_producto_tarjeta
        ,Cp_Productos_Tarjetas.cod_deli_subproducto_tarjeta
        ,Cp_Productos_Tarjetas.ds_deli_producto_tarjeta
        ,Cp_Productos_Tarjetas.cod_deli_origen_producto_tarjeta
        ,Cp_Productos_Tarjetas.cod_deli_color_producto_tarjeta
        ,Cp_Envios_Comp.MARCA_ADICIONAL as cod_deli_marca_adicional
        ,Cp_Envios_Comp.ULTIMA_MODIF as dt_deli_ultima_modificacion_componente
        ,case when Cp_Envios_Comp.CUENTAMCNRO='null' then null else Cp_Envios_Comp.CUENTAMCNRO end as cod_deli_cuentamcnro
        ,Cp_Envios_Comp.CREADO_POR as ds_deli_creador_componente
        ,Cp_Envios_Comp.ULTIMA_MODIF_POR as ds_deli_ultimo_modificador_componente
        ,Cp_Envios_Comp.MARCA_RETENIDO as cod_deli_marca_retenido
        ,case when Cp_Envios_Comp.TARJETA_RECHAZO_ABAE='null' then null else Cp_Envios_Comp.TARJETA_RECHAZO_ABAE end as cod_deli_nro_tarjeta_rechazo_abae
        ,ds_deli_color_producto_tarjeta
        ,case when Cp_Envios_Comp.secuencia='null' then null else Cp_Envios_Comp.secuencia end as cod_deli_secuencia
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'  as partition_date
FROM bi_corp_staging.rio258_Cp_Envios_Componentes Cp_Envios_Comp
left join bi_corp_common.dim_deli_cp_clientes Cp_Clientes
on trim(Cp_Envios_Comp.Cod_Cliente)=cast(Cp_Clientes.Cod_deli_Cliente as string)
left join bi_corp_common.dim_deli_cp_productos_crupier Cp_Productos_Crupier
on trim(Cp_Envios_Comp.Cod_Producto) = cast(Cp_Productos_Crupier.cod_deli_producto_componente as string)
left join bi_corp_common.dim_deli_cp_productos_tarjetas Cp_Productos_Tarjetas
on trim(Cp_Envios_Comp.Cod_Producto_Tarjeta)= cast(Cp_Productos_Tarjetas.cod_deli_prod_tarjeta as string)
LEFT JOIN bi_corp_common.dim_deli_cp_productos_colores cp_productos_colores
ON Cp_Productos_Tarjetas.cod_deli_color_producto_tarjeta=cp_productos_colores.cod_deli_color_producto_tarjeta
where Cp_Envios_Comp.ultima_modif= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}';

