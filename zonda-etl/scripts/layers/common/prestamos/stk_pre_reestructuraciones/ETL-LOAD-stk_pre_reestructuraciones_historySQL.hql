set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_pre_reestructuraciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
select
cast(risk_ree.sucursal as int) as cod_suc_sucursal,
cast(risk_ree.nro_cuenta as bigint) as cod_nro_cuenta,
cast(risk_ree.nup as int) as cod_per_nup,
risk_ree.codigo_producto as cod_prod_producto,
risk_ree.codigo_subproducto as cod_prod_subproducto,
risk_ree.divisa as cod_div_divisa,
substr(risk_ree.fecha_alta_producto,1,10) as dt_pre_fechaaltaproducto,
risk_ree.tipo_reestructuracion as ds_pre_tiporeestructuracion,
case when risk_ree.mora='true' then 1 when risk_ree.mora='false' then 0 else null end as flag_pre_mora,
case when risk_ree.vigilancia_especial='true' then 1 when risk_ree.vigilancia_especial='false' then 0 else null end as flag_pre_vigilanciaespecial,
case when risk_ree.subestandar='true' then '1' when risk_ree.subestandar='false' then '0' else null end as ds_pre_subestandar,
risk_ree.estado_gestion as ds_pre_estadogestion,
risk_ree.codigo_segmento as cod_pre_segmento,
risk_ree.descripcion_segmento  as ds_pre_segmento,
risk_ree.detalle_renta as ds_pre_renta,
risk_ree.categoria_producto as ds_pre_categoriaproducto,
case when risk_ree.normalizado='true' then 1 when risk_ree.normalizado='false' then 0 else null end as flag_pre_normalizado,
risk_ree.bucket as ds_pre_bucket,
risk_ree.tipo_clasificacion as cod_pre_tipoclasificacion,
risk_ree.descripcion_tipo_clasificacion as ds_pre_tipoclasificacion,
risk_ree.tipo_movimiento as ds_pre_tipomovimiento,
null as ds_pre_origendereestructuracion,
cast(risk_ree.cantidad_reestructuraciones_sucesivas as int) as fc_pre_reestructuracionsucesiva,
cast(risk_ree.dias_atraso as int) as fc_pre_diasatraso,
cast(risk_ree.importe_riesgo as decimal(15,2)) as fc_pre_importeriesgo,
cast(risk_ree.importe_dispuesto as decimal(15,2)) as fc_pre_importedispuesto,
cast(risk_ree.porcentaje_prestamo_hipotecario as decimal(15,2)) as fc_pre_porcentajeprestamohipotecario,
cast(risk_ree.porcentaje_prestamo_prendario as decimal(15,2)) as fc_pre_porcentajeprestamoprendario,
cast(risk_ree.porcentaje_personales as decimal(15,2)) as fc_pre_porcentajeprestamopersonales,
cast(risk_ree.porcentaje_tarjeta_credito as decimal(15,2)) as fc_pre_porcentajetarjetacredito,
cast(risk_ree.porcentaje_cuenta_corriente as decimal(15,2)) as fc_pre_porcentajecuentacorriente,
cast(risk_ree.porcentaje_otro_producto as decimal(15,2)) as fc_pre_porcentajeotroproducto,
cast(risk_ree.porc_deuda_pendiente_pago as decimal(15,2)) as fc_pre_porcentajedeudapendientepago,
cast(risk_ree.porc_deuda_pagada as decimal(15,2)) as fc_pre_porcentajedeudapagada,
null as flag_pre_marcacovid
from bi_corp_staging.risksql5_stk_reestructuraciones risk_ree
where partition_date=''{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';