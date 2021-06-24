set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_deli_envios
partition(partition_date)
SELECT
         Cp_Envios.CRUPIER_ID as cod_deli_crupier
        ,Cp_Clientes.cod_per_nup
        ,cast(Cp_Envios.COD_SUCURSAL as int) as cod_suc_sucursal
        ,case when Cp_Envios.NRO_SOLICITUD_ASOL='null' then null else Cp_Envios.NRO_SOLICITUD_ASOL end as cod_deli_solicitud
        ,case when Cp_Envios.NRO_PAQUETE='null' then null else Cp_Envios.NRO_PAQUETE end as cod_deli_nro_paquete
        ,case when Cp_Envios.NRO_CTA_PAQUETE='null' then null else Cp_Envios.NRO_CTA_PAQUETE end as cod_deli_nro_cta_paquete
        ,Cp_Envios.COD_OPERACION as cod_deli_operacion
        ,Cp_Operaciones_Crupier.ds_deli_operacion
        ,date_format(Cp_Envios.FECHA_REGISTRO,'yyyy-MM-dd') as dt_deli_registro
        ,case when Cp_Envios.COD_CANAL_VENTA='null' then null else Cp_Envios.COD_CANAL_VENTA end as cod_deli_canal_venta
        ,Cp_Canales_venta.ds_deli_canal_venta
        ,case when Cp_Envios.EJECUTIVO_COMERCIAL='null' then null else Cp_Envios.EJECUTIVO_COMERCIAL end as cod_deli_ejecutivo_comercial
        ,case when Cp_Envios.CALLE='null' then null else Cp_Envios.CALLE end as ds_deli_calle
        ,case when Cp_Envios.ALTURA='null' then null else Cp_Envios.ALTURA end as cod_deli_altura
        ,case when Cp_Envios.PISO='null' then null else Cp_Envios.PISO end as cod_deli_piso
        ,case when Cp_Envios.DEPTO='null' then null else Cp_Envios.DEPTO end as ds_deli_depto
        ,case when Cp_Envios.COD_POSTAL='null' then null else Cp_Envios.COD_POSTAL end as cod_deli_postal
        ,case when Cp_Envios.LOCALIDAD='null' then null else Cp_Envios.LOCALIDAD end as ds_deli_localidad
        ,case when Cp_Envios.TELEFONO='null' then null else Cp_Envios.TELEFONO end as ds_deli_telefono
        ,case when Cp_Envios.COD_PAQUETE='null' then null else Cp_Envios.COD_PAQUETE end as cod_deli_paquete
        ,Cp_Paquetes_Crupier.ds_deli_paquete_crupier
        ,case when Cp_Envios.COD_BARRA='null' then null else Cp_Envios.COD_BARRA end as cod_deli_codigo_barra
        ,Cp_Envios.ULTIMA_MODIF as dt_deli_ultima_modificacion
        ,Cp_Envios.CREADO_POR as ds_deli_creador
        ,Cp_Envios.ULTIMA_MODIF_POR as ds_deli_ultimo_modificador
        ,case when Cp_Envios.COD_PRODUCTO='null' then null else Cp_Envios.COD_PRODUCTO end as cod_deli_producto
        ,case when Cp_Envios.cod_subproducto='null' then null else Cp_Envios.cod_subproducto end as cod_deli_subproducto
        ,case when Cp_Envios.TIPO_DOM='null' then null else Cp_Envios.TIPO_DOM end as ds_deli_tipo_dom
        ,case when Cp_Envios.EXP_PARTIAL='null' then null else Cp_Envios.EXP_PARTIAL end as cod_deli_exp_partial
        ,case when Cp_Envios.es_finalizado='null' then null else Cp_Envios.es_finalizado end as flag_deli_finalizado
        ,case when Cp_Envios.courier='null' then null else Cp_Envios.courier end as ds_deli_courier
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
FROM bi_corp_staging.rio258_Cp_Envios Cp_Envios
left join bi_corp_common.dim_deli_cp_clientes Cp_Clientes
on trim(Cp_Envios.Cod_Cliente)=cast(Cp_Clientes.cod_deli_cliente as string)
left join bi_corp_common.dim_deli_cp_oper_crupier Cp_Operaciones_Crupier
on trim(Cp_Envios.Cod_Operacion)=cast(Cp_Operaciones_Crupier.cod_deli_operacion as string)
left join bi_corp_common.dim_deli_cp_canales_venta Cp_Canales_venta
on trim(Cp_Envios.cod_canal_venta) = Cp_Canales_venta.cod_deli_canal_venta
left join bi_corp_common.dim_deli_cp_paquetes_crupier Cp_Paquetes_Crupier
on trim(Cp_Envios.Cod_Paquete) = cast(Cp_Paquetes_Crupier.cod_deli_paquete_crupier as string)
where Cp_Envios.ultima_modif = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
;
