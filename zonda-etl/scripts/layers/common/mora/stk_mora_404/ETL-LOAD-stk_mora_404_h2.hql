set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


CREATE TEMPORARY TABLE mora_6618 AS
SELECT
case when w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_MORA_History2') }}' then '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA_History2') }}' else null end AS periodo,
w.waguxdex_cod_entidad AS cod_entidad,
w.waguxdex_num_persona AS nup,
w.waguxdex_cod_centro AS cod_sucursal,
w.waguxdex_num_contrato AS num_cuenta,
w.waguxdex_cod_producto AS cod_producto,
w.waguxdex_cod_subprodu AS cod_subproducto,
w.waguxdex_cod_divisa AS cod_divisa,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN NULL ELSE w.waguxdex_fec_inisitir END AS fecha_situacion_irregular,
w.waguxdex_fec_alt_prod AS fecha_alta_producto,
w.waguxdex_fec_vto_prod AS fecha_vto_producto,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN 0 ELSE DATEDIFF(to_date(w.partition_date), to_date(w.waguxdex_fec_inisitir)) END AS dias_de_atraso_calculado,
w.waguxdex_con_diaimpag AS dias_de_atraso,
w.waguxdex_cod_marca AS cod_marca,
w.waguxdex_cod_smarcgsi AS cod_submarca,
pe.cod_per_segmentoduro AS cod_segmento,
s.segmento_control as segmento_control,
s.detalle_renta,
CASE WHEN s.segmento_control LIKE 'INDI%' THEN p.categoria_particulares ELSE p.categoria_carterizados END AS tipo_producto,
p.descripcion AS descripcion_producto,
CASE WHEN w.waguxdex_tip_reestruct IN ('02','2','REFINANCIACION') THEN 'REFINANCIACION' WHEN w.waguxdex_tip_reestruct IN ('03','3','ACUERDO DE PAGO') THEN 'ACUERDO DE PAGO' WHEN w.waguxdex_tip_reestruct IN ('01','1','RECONDUCION') THEN 'RECONDUCION' ELSE w.waguxdex_tip_reestruct END AS tipo_reestructuracion,
w.waguxdex_motivo_riesgo_sub_est AS motivo_riesgo_subestado,
w.waguxdex_cod_atrib_seg_esp AS clasificacion_reestructuracion,
w.waguxdex_fec_cambio_nor AS fecha_clasificacion_reestructuracion,
'6618' AS via_ingreso,
w.waguxdex_imp_riesmolo AS importe_riesgo,
w.waguxdex_imp_irremolo AS importe_irregular
FROM bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(w.waguxdex_cod_producto) = trim(p.codigo_producto) and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
WHERE w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}' and
w.waguxdex_cod_marca != 'FA' and
w.waguxdex_imp_riesmolo > 0 and
(
w.waguxdex_cod_atrib_seg_esp = 'DU'
or
(p.refinanciacion = 'true' and
(
trim(w.waguxdex_cod_smarcgsi ) in ('ACL', 'PAS') or
(w.waguxdex_tip_reestruct in ('02','03','2','3','REFINANCIACION', 'ACUERDO DE PAGO') and trim(coalesce(w.waguxdex_cod_atrib_seg_esp, '')) = '') or
(trim(p.tipo_reestructuracion) = 'GENERICO' and trim(coalesce(w.waguxdex_cod_atrib_seg_esp, '')) = '' and trim(w.waguxdex_tip_reestruct) in ('02','03','2','3','REFINANCIACION', 'ACUERDO DE PAGO'))
)
)
)
;


CREATE TEMPORARY TABLE mora_garra AS
SELECT
case when w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_MORA_History2') }}' then '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA_History2') }}' else null end AS periodo,
w.waguxdex_cod_entidad AS cod_entidad,
w.waguxdex_num_persona AS nup,
w.waguxdex_cod_centro AS cod_sucursal,
w.waguxdex_num_contrato AS num_cuenta,
w.waguxdex_cod_producto AS cod_producto,
w.waguxdex_cod_subprodu AS cod_subproducto,
w.waguxdex_cod_divisa AS cod_divisa,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN NULL ELSE w.waguxdex_fec_inisitir END AS fecha_situacion_irregular,
w.waguxdex_fec_alt_prod AS fecha_alta_producto,
w.waguxdex_fec_vto_prod AS fecha_vto_producto,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN 0 ELSE DATEDIFF(to_date(w.partition_date), to_date(w.waguxdex_fec_inisitir)) END AS dias_de_atraso_calculado,
w.waguxdex_con_diaimpag AS dias_de_atraso,
w.waguxdex_cod_marca AS cod_marca,
w.waguxdex_cod_smarcgsi AS cod_submarca,
pe.cod_per_segmentoduro AS cod_segmento,
s.segmento_control as segmento_control,
s.detalle_renta,
CASE WHEN s.segmento_control LIKE 'INDI%' THEN p.categoria_particulares ELSE p.categoria_carterizados END AS tipo_producto,
p.descripcion AS descripcion_producto,
p.tipo_reestructuracion AS tipo_reestructuracion,
w.waguxdex_motivo_riesgo_sub_est AS motivo_riesgo_subestado,
w.waguxdex_cod_atrib_seg_esp AS clasificacion_reestructuracion,
w.waguxdex_fec_cambio_nor AS fecha_clasificacion_reestructuracion,
'GARRA' AS via_ingreso,
w.waguxdex_imp_riesmolo AS importe_riesgo,
w.waguxdex_imp_irremolo AS importe_irregular
FROM bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(w.waguxdex_cod_producto) = trim(p.codigo_producto) and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN mora_6618 m6 ON m6.cod_entidad = w.waguxdex_cod_entidad and m6.cod_sucursal = w.waguxdex_cod_centro and m6.num_cuenta = w.waguxdex_num_contrato and m6.cod_producto = w.waguxdex_cod_producto and m6.cod_subproducto = w.waguxdex_cod_subprodu and m6.cod_divisa = w.waguxdex_cod_divisa
WHERE w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}' and
w.waguxdex_cod_marca != 'FA' and
w.waguxdex_imp_riesmolo > 0 and
(
(w.waguxdex_cod_subprodu IN('0025','0044','0054','0064' ) AND w.waguxdex_cod_producto='71' ) OR--Agregado por Refinanciaciones sobre Prestamos Securitizados
(w.waguxdex_cod_marca  IN ('MO','PR','CO','DE') AND pe.cod_per_division NOT IN ('50','31','80','81' ,'82','60')  AND (ds_per_nombre NOT LIKE '%BANCO RIO DE LA PLATA%')) OR
(w.waguxdex_cod_producto='70')
)
AND m6.num_cuenta IS NULL -- excluyo los casos que entraron por el 6618
;


CREATE TEMPORARY TABLE marca_comite AS
select c.periodo, c.nup, c.cuit, c.razon_social, c.bde_clasificacion_final,
case TRIM(c.bde_clasificacion_final) when 'CONTENCIOSO' then 'CO'
when 'DNP' then 'DE'
when 'SUBST' then 'SU'
when 'PRECONTENCIOSO' then 'PR'
when 'FALLIDO' then 'FA'
when 'MOROSO' then 'MO'
when 'NO MORA' then 'NO'
when '' then 'MOR'
else null end as marca,
case TRIM(c.bde_clasificacion_final) when 'CONTENCIOSO' then 'CON'
when 'DNP' then 'GEN'
when 'SUBST' then 'SUB'
when 'PRECONTENCIOSO' then 'CON'
when 'FALLIDO' then 'FA'
when 'MOROSO' then 'SIM'
when 'NO MORA' then 'MOR'
when '' then 'MOR'
else null end as submarca
from bi_corp_staging.risksql5_comite c
where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
and c.periodo = (case when c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_MORA_History2') }}' then  '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA_History2') }}' else '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='LOAD_CMN_MORA_History2') }}' end)
and c.bde_clasificacion_final NOT IN ('ELIMINADA', 'CONTABILI', 'Normal', 'Liberar')
and c.excluido = 'false'
;



CREATE TEMPORARY TABLE mora_comite AS
SELECT
case when w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_MORA_History2') }}' then '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA_History2') }}' else null end AS periodo,
w.waguxdex_cod_entidad AS cod_entidad,
w.waguxdex_num_persona AS nup,
w.waguxdex_cod_centro AS cod_sucursal,
w.waguxdex_num_contrato AS num_cuenta,
w.waguxdex_cod_producto AS cod_producto,
w.waguxdex_cod_subprodu AS cod_subproducto,
w.waguxdex_cod_divisa AS cod_divisa,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN NULL ELSE w.waguxdex_fec_inisitir END AS fecha_situacion_irregular,
w.waguxdex_fec_alt_prod AS fecha_alta_producto,
w.waguxdex_fec_vto_prod AS fecha_vto_producto,
CASE WHEN YEAR(w.waguxdex_fec_inisitir) <= 1901 THEN 0 ELSE DATEDIFF(to_date(w.partition_date), to_date(w.waguxdex_fec_inisitir)) END AS dias_de_atraso_calculado,
w.waguxdex_con_diaimpag AS dias_de_atraso,
mc.marca AS cod_marca,
mc.submarca AS cod_submarca,
pe.cod_per_segmentoduro AS cod_segmento,
s.segmento_control as segmento_control,
s.detalle_renta,
CASE WHEN s.segmento_control LIKE 'INDI%' THEN p.categoria_particulares ELSE p.categoria_carterizados END AS tipo_producto,
p.descripcion AS descripcion_producto,
p.tipo_reestructuracion AS tipo_reestructuracion,
w.waguxdex_motivo_riesgo_sub_est AS motivo_riesgo_subestado,
w.waguxdex_cod_atrib_seg_esp AS clasificacion_reestructuracion,
w.waguxdex_fec_cambio_nor AS fecha_clasificacion_reestructuracion,
'COMITE' AS via_ingreso,
w.waguxdex_imp_riesmolo AS importe_riesgo,
w.waguxdex_imp_irremolo AS importe_irregular
FROM bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(w.waguxdex_cod_producto) = trim(p.codigo_producto) and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}'
INNER JOIN marca_comite mc ON lpad(cast(mc.nup as string), 8, '0') = w.waguxdex_num_persona
LEFT JOIN mora_6618 m6 ON m6.cod_entidad = w.waguxdex_cod_entidad and m6.cod_sucursal = w.waguxdex_cod_centro and m6.num_cuenta = w.waguxdex_num_contrato and m6.cod_producto = w.waguxdex_cod_producto and m6.cod_subproducto = w.waguxdex_cod_subprodu and m6.cod_divisa = w.waguxdex_cod_divisa
LEFT JOIN mora_garra mg ON mg.cod_entidad = w.waguxdex_cod_entidad and mg.cod_sucursal = w.waguxdex_cod_centro and mg.num_cuenta = w.waguxdex_num_contrato and mg.cod_producto = w.waguxdex_cod_producto and mg.cod_subproducto = w.waguxdex_cod_subprodu and mg.cod_divisa = w.waguxdex_cod_divisa
WHERE w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}' and
w.waguxdex_cod_marca != 'FA' and
w.waguxdex_imp_riesmolo > 0 and
mc.marca NOT IN ('NO', 'ER', 'FA')
AND m6.num_cuenta IS NULL -- excluyo los casos que entraron por el 6618
AND mg.num_cuenta IS NULL -- excluyo los casos que entraron por garra
;


CREATE TEMPORARY TABLE egresos_comite AS
SELECT DISTINCT lpad(cast(mc.nup as string), 8, '0') as nup
FROM marca_comite mc
WHERE mc.marca IN ('NO', 'ER', 'FA')
;


CREATE TEMPORARY TABLE ingresos_comite AS
SELECT *
FROM mora_6618
UNION ALL
SELECT *
FROM mora_garra
UNION ALL
SELECT *
FROM mora_comite
;


insert overwrite table bi_corp_common.stk_mora_404
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_History2') }}' )

SELECT
i.periodo,
i.cod_entidad,
cast(i.nup as bigint) as nup,
cast(i.cod_sucursal as bigint) as sucursal,
cast(i.num_cuenta as bigint) as num_cuenta,
i.cod_producto,
i.cod_subproducto,
i.cod_divisa,
case when to_date(i.fecha_situacion_irregular) in ('9999-12-31', '0001-01-01') then NULL else to_date(i.fecha_situacion_irregular) end as fecha_situacion_irregular,
trim(i.fecha_alta_producto) as fecha_alta_producto,
case when to_date(i.fecha_vto_producto) in ('9999-12-31', '0001-01-01') then NULL else to_date(i.fecha_vto_producto) end as fecha_vto_producto,
i.dias_de_atraso_calculado,
cast(i.dias_de_atraso as int) as dias_de_atraso,
i.cod_marca,
i.cod_submarca,
i.cod_segmento,
i.segmento_control,
i.detalle_renta,
i.tipo_producto,
i.descripcion_producto,
i.tipo_reestructuracion,
i.motivo_riesgo_subestado,
i.clasificacion_reestructuracion,
case when to_date(i.fecha_clasificacion_reestructuracion) in ('9999-12-31', '0001-01-01') then NULL else to_date(i.fecha_clasificacion_reestructuracion) end as fecha_clasificacion_reestructuracion,
i.via_ingreso,
cast(i.importe_riesgo as DECIMAL(17,4)) as importe_riesgo,
cast(i.importe_irregular as DECIMAL(17,4)) as importe_irregular
FROM ingresos_comite i
LEFT JOIN egresos_comite e ON i.nup = e.nup
WHERE e.nup IS NULL
;