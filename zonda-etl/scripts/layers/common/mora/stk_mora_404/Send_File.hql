"set mapred.job.queue.name=root.dataeng;
SELECT
periodo,
cod_entidad,
nup,
cod_sucursal,
num_cuenta,
cod_producto,
cod_subproducto,
cod_divisa,
fecha_situacion_irregular,
fecha_alta_producto,
fecha_vto_producto,
dias_de_atraso_calculado,
dias_de_atraso,
cod_marca,
cod_submarca,
cod_segmento,
segmento_control,
detalle_renta,
tipo_producto,
descripcion_producto,
tipo_reestructuracion,
motivo_riesgo_subestado,
clasificacion_reestructuracion,
fecha_clasificacion_reestructuracion,
via_ingreso,
importe_riesgo,
importe_irregular,
partition_date
FROM bi_corp_common.stk_mora_404
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
"