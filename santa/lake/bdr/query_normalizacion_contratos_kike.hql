set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


WITH productos_riesgos AS (
select ma.cod_producto, p.desc_40
from santander_business_risk_arda.maestro_atributos ma, santander_business_risk_arda.producto p
where ma.data_date_part = '20190930'
and p.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and p.cod_producto = ma.cod_producto
GROUP BY ma.cod_producto, p.desc_40
)
INSERT OVERWRITE TABLE bi_corp_bdr.normalizacion_id_contratos_test
PARTITION ( partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_NormalizacionBDR-Prov') }}' )
SELECT
CASE WHEN nor.id_cto_bdr IS NOT NULL THEN nor.id_cto_bdr
ELSE lpad(CAST(COALESCE(LAG(nor.id_cto_bdr) OVER (PARTITION BY nor.id_cto_bdr),0) + ROW_NUMBER() OVER() AS INT), 9, '0')
END id_cto_bdr , dist.*
FROM (
SELECT DISTINCT cto.* FROM (
SELECT 'credito' source
, concat_ws('_', a.cod_entidad, a.cod_centro , a.num_cuenta, a.cod_producto, a.cod_subprodu_altair , a.cod_divisa) id_cto_source
, a.cod_entidad cred_cod_entidad
, a.cod_centro cred_cod_centro
, a.num_cuenta cred_num_cuenta
, a.cod_producto cred_cod_producto
, a.cod_subprodu_altair cred_cod_subprodu_altair
, a.cod_divisa cred_cod_divisa
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM santander_business_risk_arda.contratos a
WHERE a.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_NormalizacionBDR-Prov') }}'
AND NOT (a.cod_estado_rel_cliente = 'C' and a.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='LOAD_NormalizacionBDR-Prov') }}' )
AND (a.cod_marca != "FA" or a.cod_marca is null)
UNION ALL
SELECT 'credito' source
, concat_ws('_', core.pecdgent, core.pecodofi, core.penumcon, core.pecodpro, core.pecodsub, core.pecodmon) id_cto_source
, core.pecdgent cred_cod_entidad
, core.pecodofi cred_cod_centro
, core.penumcon cred_num_cuenta
, core.pecodpro cred_cod_producto
, core.pecodsub cred_cod_subprodu_altair
, core.pecodmon cred_cod_divisa
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_pedt042 core
INNER JOIN productos_riesgos p on p.cod_producto = core.pecodpro
WHERE core.partition_date = '2020-04-02'
AND core.PEESTOPE = 'C'
AND concat(substr(core.PEFECEST,1,4),substr(core.PEFECEST,6,2)) = substr('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='LOAD_NormalizacionBDR-Prov') }}',1,6)
UNION ALL
SELECT 'credito' source
, concat_ws('_', a.cod_entidad, a.cod_centro , a.num_cuenta, a.cod_producto, a.cod_subprodu_altair , a.cod_divisa) id_cto_source
, a.cod_entidad cred_cod_entidad
, a.cod_centro cred_cod_centro
, a.num_cuenta cred_num_cuenta
, a.cod_producto cred_cod_producto
, a.cod_subprodu_altair cred_cod_subprodu_altair
, a.cod_divisa cred_cod_divisa
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM santander_business_risk_arda.contratos a
WHERE a.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='LOAD_NormalizacionBDR-Prov') }}' AND a.cod_marca = "FA" AND substr(a.fec_castigo,1,7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
SELECT 'balance sociedad' source
, concat_ws('_', core.sociedad, core.cargabal, core.sociedad_eliminacion) id_cto_source
, '' cred_cod_entidad, '' cred_cod_centro, '' cred_num_cuenta, '' cred_cod_producto
, '' cred_cod_subprodu_altair , '' cred_cod_divisa, '' mmff_cod_especie, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen, '' mmff_cod_cartera
, '' mmff_cod_operacion, '' mmff_cod_pata, core.sociedad blc_sociedad, core.cargabal blc_cargabal
, core.sociedad_eliminacion blc_sociedad_eliminacion
FROM (
SELECT cargabal, sociedad, sociedad_eliminacion
FROM bi_corp_bdr.balances_ajustes
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_NormalizacionBDR-Prov') }}'
GROUP BY cargabal, sociedad, sociedad_eliminacion) core
UNION ALL
-- CONTRATO VIEJO
SELECT
'credito' source,
concat_ws('_', r.cod_entidadg, r.cod_centrog , r.num_contratg, r.cod_productg, r.cod_subprodg, r.cod_divisag) id_cto_source,
r.cod_entidadg as cred_cod_entidad,
r.cod_centrog  as cred_cod_centro,
r.num_contratg as cred_num_cuenta,
r.cod_productg as cred_cod_producto,
r.cod_subprodg as cred_cod_subprodu_altair,
r.cod_divisag  as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_productg
WHERE r.partition_date = '2020-03-27' and r.fec_altareg between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
SELECT
'credito' source,
concat_ws('_', r.mig_old_entidad, r.mig_old_cent_alta , concat('000', substring(r.mig_old_cuenta, 4, 12)), p.pecodpro, p.pecodsub, r.mig_old_divisa) id_cto_source,
r.mig_old_entidad as cred_cod_entidad,
r.mig_old_cent_alta as cred_cod_centro,
concat('000', substring(r.mig_old_cuenta, 4, 12)) as cred_num_cuenta,
p.pecodpro as cred_cod_producto,
p.pecodsub as cred_cod_subprodu_altair,
r.mig_old_divisa as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p on p.pecodent = r.mig_old_entidad and p.pecodofi = r.mig_old_cent_alta and p.penumcon = concat('000', substring(r.mig_old_cuenta, 4, 12) ) and r.partition_date = p.partition_date
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
WHERE r.partition_date = '2020-03-27' and p.partition_date = '2020-03-27' and r.mig_old_fech_baja between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
SELECT
'credito' source,
concat_ws('_', r.pecdgent, r.datos_comunes_peofiori , concat('000', substring(r.penumcon, 4)), r.pecodpro, r.pecodsub, r.pecodmon) id_cto_source,
r.pecdgent as cred_cod_entidad,
r.datos_comunes_peofiori as cred_cod_centro,
concat('000', substring(r.penumcon, 4)) as cred_num_cuenta,-- AGREGAR CEROS
r.pecodpro as cred_cod_producto,
r.pecodsub as cred_cod_subprodu_altair,
r.pecodmon as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.pecodpro
WHERE r.partition_date = '2019-12-13' and r.pefecest between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
-- CONTRATO NUEVO
SELECT
'credito' source,
concat_ws('_', r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, r.cod_subprodu, r.cod_divisa) id_cto_source,
r.cod_entidad as cred_cod_entidad,
r.cod_centro as cred_cod_centro,
r.num_contrato as cred_num_cuenta,
r.cod_producto as cred_cod_producto,
r.cod_subprodu as cred_cod_subprodu_altair,
r.cod_divisa as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_producto
WHERE r.partition_date = '2020-03-27' and r.fec_altareg between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
SELECT
'credito' source,
concat_ws('_', r.mig_new_entidad, r.mig_new_cent_alta, concat('000', substring(r.mig_new_cuenta, 4, 12)), p.pecodpro, p.pecodsub, r.mig_new_divisa) id_cto_source,
r.mig_new_entidad as cred_cod_entidad,
r.mig_new_cent_alta as cred_cod_centro,
concat('000', substring(r.mig_new_cuenta, 4, 12)) as cred_num_cuenta,
p.pecodpro as cred_cod_producto,
p.pecodsub as cred_cod_subprodu_altair,
r.mig_new_divisa as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p on p.pecodent = r.mig_old_entidad and p.pecodofi = r.mig_old_cent_alta and p.penumcon = concat('000', substring(r.mig_old_cuenta, 4, 12) ) and r.partition_date = p.partition_date
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
WHERE r.partition_date = '2020-03-27' and p.partition_date = '2020-03-27' and r.mig_old_fech_baja between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
UNION ALL
SELECT
'credito' source,
concat_ws('_', r.pecdgentd, r.datos_comunes_peofides, concat('000', substring(r.penumcond, 4)), r.pecodprod, r.pecodsubd, r.pecodmond) id_cto_source,
r.pecdgentd as cred_cod_entidad,
r.datos_comunes_peofides as cred_cod_centro,
concat('000', substring(r.penumcond, 4)) as cred_num_cuenta,
r.pecodprod as cred_cod_producto,
r.pecodsubd as cred_cod_subprodu_altair,
r.pecodmond as cred_cod_divisa,
'' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos p on p.cod_producto = r.pecodprod
WHERE r.partition_date = '2019-12-13' and r.pefecest between
'{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
and
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_NormalizacionBDR-Prov') }}'
)cto
)dist
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history nor ON nor.id_cto_source = dist.id_cto_source;


INSERT INTO TABLE bi_corp_bdr.normalizacion_id_contratos_history
SELECT DISTINCT
n.id_cto_bdr,
n.source,
n.id_cto_source,
n.cred_cod_entidad,
n.cred_cod_centro,
n.cred_num_cuenta,
n.cred_cod_producto,
n.cred_cod_subprodu_altair,
n.cred_cod_divisa,
n.mmff_cod_especie,
n.mmff_cod_disponibilidad,
n.mmff_cod_sis_origen,
n.mmff_cod_cartera,
n.mmff_cod_operacion,
n.mmff_cod_pata,
n.blc_sociedad,
n.blc_cargabal,
n.blc_sociedad_eliminacion,
current_date as inserted_date
FROM bi_corp_bdr.normalizacion_id_contratos_test n
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history h ON h.id_cto_bdr = n.id_cto_bdr
WHERE n.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_NormalizacionBDR-Prov') }}'
AND h.id_cto_bdr IS NULL;
