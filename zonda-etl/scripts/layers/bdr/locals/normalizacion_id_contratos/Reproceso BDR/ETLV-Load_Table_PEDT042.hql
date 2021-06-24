set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE productos_riesgos AS
select ma.cod_producto
from santander_business_risk_arda.maestro_atributos ma
where ma.data_date_part = '20190930'
GROUP BY ma.cod_producto;


-- tabla temporal que genera el perímetro de elementos a mapear en la normalizacion.
CREATE TEMPORARY TABLE perimetro_contratos AS
SELECT DISTINCT contratos.* FROM (
-- asociado a contratos bis
SELECT 'credito' source
, concat_ws('_', a.cod_entidad, a.cod_centro , a.num_cuenta, a.cod_producto, a.cod_subprodu_altair) id_cto_source
, a.cod_entidad cred_cod_entidad
, a.cod_centro cred_cod_centro
, a.num_cuenta cred_num_cuenta
, a.cod_producto cred_cod_producto
, a.cod_subprodu_altair cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM santander_business_risk_arda.contratos a
--WHERE a.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
WHERE a.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
--AND (a.cod_estado_rel_cliente is null or (NOT (a.cod_estado_rel_cliente = 'C' and a.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')))
AND (a.cod_estado_rel_cliente is null or (NOT (a.cod_estado_rel_cliente = 'C' and a.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')))
AND (a.cod_marca != 'FA' or a.cod_marca is null)
UNION ALL
-- asociado a contratos cancelados
SELECT 'credito' source
, concat_ws('_', core.pecdgent, core.pecodofi, core.penumcon, core.pecodpro, trim(core.pecodsub)) id_cto_source
, core.pecdgent cred_cod_entidad
, core.pecodofi cred_cod_centro
, core.penumcon cred_num_cuenta
, core.pecodpro cred_cod_producto
, trim(core.pecodsub) cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_pedt042 core
INNER JOIN productos_riesgos p on p.cod_producto = core.pecodpro
--WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
WHERE core.partition_date = '2019-12-04'
AND core.PEESTOPE = 'C'
and substr(core.PEFECEST,1,7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perímetro de contratos fallidos
SELECT 'credito' source
, concat_ws('_', a.cod_entidad, a.cod_centro , a.num_cuenta, a.cod_producto, a.cod_subprodu_altair) id_cto_source
, a.cod_entidad cred_cod_entidad
, a.cod_centro cred_cod_centro
, a.num_cuenta cred_num_cuenta
, a.cod_producto cred_cod_producto
, a.cod_subprodu_altair cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM santander_business_risk_arda.contratos a
WHERE a.data_date_part between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                               and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  AND a.cod_marca = "FA"
  AND substr(a.fec_castigo,1,7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de exposiciones no contractuales
SELECT 'balance sociedad' source
, concat_ws('_', core.sociedad, core.cargabal, core.sociedad_eliminacion) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, core.sociedad blc_sociedad
, core.cargabal blc_cargabal
, core.sociedad_eliminacion blc_sociedad_eliminacion
FROM (
SELECT cargabal, sociedad, sociedad_eliminacion
FROM bi_corp_bdr.balances_ajustes
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
GROUP BY cargabal, sociedad, sociedad_eliminacion) core
UNION ALL
-- perimetro de renumeraciones (CONTRATO VIEJO)
SELECT
'credito' source
, concat_ws('_', r.cod_entidadg, r.cod_centrog , r.num_contratg, r.cod_productg, trim(r.cod_subprodg)) id_cto_source
, r.cod_entidadg cred_cod_entidad
, r.cod_centrog  cred_cod_centro
, r.num_contratg cred_num_cuenta
, r.cod_productg cred_cod_producto
, trim(r.cod_subprodg) cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_productg
WHERE r.partition_date = '2019-12-19' -- valor fijo para procesar hasta DICIEMBRE 19
  and r.fec_altareg between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                            and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de renumeraciones (CONTRATO VIEJO)
SELECT
'credito' source
, concat_ws('_', r.mig_old_entidad, r.mig_old_cent_alta , concat('000', substring(r.mig_old_cuenta, 4, 12)), p.pecodpro, trim(p.pecodsub)) id_cto_source
, r.mig_old_entidad cred_cod_entidad
, r.mig_old_cent_alta cred_cod_centro
, concat('000', substring(r.mig_old_cuenta, 4, 12)) cred_num_cuenta
, p.pecodpro cred_cod_producto
, trim(p.pecodsub) cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p
      on p.pecodent = r.mig_old_entidad
     and p.pecodofi = r.mig_old_cent_alta
     and concat("0",p.pecodpro, substring(p.penumcon, 4))  = r.mig_old_cuenta
--     and p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
     and p.partition_date = '2019-12-04'
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
--WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_zbdtmig', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and r.mig_old_fech_baja between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                                  and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de renumeraciones (CONTRATO VIEJO)
SELECT
'credito' source
, concat_ws('_', r.pecdgent, r.datos_comunes_peofiori , concat('000', substring(r.penumcon, 4)), r.pecodpro, trim(r.pecodsub)) id_cto_source
, r.pecdgent cred_cod_entidad
, r.datos_comunes_peofiori cred_cod_centro
, concat('000', substring(r.penumcon, 4)) cred_num_cuenta
, r.pecodpro cred_cod_producto
, trim(r.pecodsub) cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.pecodpro
WHERE r.partition_date = '2019-12-13'
  and r.pefecest between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                         and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de renumeraciones (CONTRATO NUEVO)
SELECT
'credito' source
, concat_ws('_', r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, r.cod_subprodu) id_cto_source
, r.cod_entidad cred_cod_entidad
, r.cod_centro cred_cod_centro
, r.num_contrato cred_num_cuenta
, r.cod_producto cred_cod_producto
, r.cod_subprodu cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_producto
WHERE r.partition_date = '2019-12-19' -- valor fijo para procesar hasta DICIEMBRE 19
  and r.fec_altareg between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                            and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de renumeraciones (CONTRATO NUEVO)
SELECT
'credito' source
, concat_ws('_', r.mig_new_entidad, r.mig_new_cent_alta, concat('000', substring(r.mig_new_cuenta, 4, 12)), p.pecodpro, trim(p.pecodsub)) id_cto_source
, r.mig_new_entidad cred_cod_entidad
, r.mig_new_cent_alta cred_cod_centro
, concat('000', substring(r.mig_new_cuenta, 4, 12)) cred_num_cuenta
, p.pecodpro cred_cod_producto
, trim(p.pecodsub) cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p
       on p.pecodent = r.mig_old_entidad
      and p.pecodofi = r.mig_old_cent_alta
      and concat("0",p.pecodpro, substring(p.penumcon, 4) )  = r.mig_old_cuenta
      and p.partition_date = '2019-12-04'
--      and p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_zbdtmig', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
--WHERE r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and r.mig_old_fech_baja between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                                  and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
UNION ALL
-- perimetro de renumeraciones (CONTRATO NUEVO)
SELECT
'credito' source
, concat_ws('_', r.pecdgentd, r.datos_comunes_peofides, concat('000', substring(r.penumcond, 4)), r.pecodprod, trim(r.pecodsubd)) id_cto_source
, r.pecdgentd cred_cod_entidad
, r.datos_comunes_peofides cred_cod_centro
, concat('000', substring(r.penumcond, 4)) cred_num_cuenta
, r.pecodprod cred_cod_producto
, trim(r.pecodsubd) cred_cod_subprodu_altair
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos p on p.cod_producto = r.pecodprod
WHERE r.partition_date = '2019-12-13'
and r.pefecest between     '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                       and '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
-- perimetro de MMFF
-- ESPECIES
UNION ALL
SELECT 'mmff - especies' source
, concat_ws('_', trim(tesp.cod_especie), trim(tesp.cod_disponibilidad), trim(tesp.cod_sis_origen), trim(tesp.cod_cartera)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, trim(tesp.cod_especie) mmff_cod_especie
, trim(tesp.cod_disponibilidad) mmff_cod_disponibilidad
, trim(tesp.cod_sis_origen) mmff_cod_sis_origen
, trim(tesp.cod_cartera) mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from santander_business_risk_arda.especies tesp
inner join
     (select max(a.data_date_part) as data_date_part, a.fec_info, a.cod_cartera, a.cod_especie, a.cod_sis_origen, a.cod_disponibilidad
        from santander_business_risk_arda.especies a
      inner join
                (select max(b.fec_info) as fec_info, b.cod_cartera, b.cod_especie, b.cod_sis_origen, b.cod_disponibilidad
                   from santander_business_risk_arda.especies b
                  where b.data_date_part <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                    and b.fec_info <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                    and b.cod_cartera in ('ALC', 'INV')
                  group by b.cod_cartera, b.cod_especie,  b.cod_sis_origen, b.cod_disponibilidad) c
             on c.fec_info    = a.fec_info
            and c.cod_cartera = a.cod_cartera
            and c.cod_especie = a.cod_especie
            and c.cod_sis_origen = a.cod_sis_origen
            and c.cod_disponibilidad = a.cod_disponibilidad
       where a.data_date_part <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         and a.fec_info <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
       group by a.fec_info, a.cod_cartera, a.cod_especie, a.cod_sis_origen, a.cod_disponibilidad) in_pk
     on in_pk.data_date_part = tesp.data_date_part
    and in_pk.fec_info = tesp.fec_info
    and in_pk.cod_cartera = tesp.cod_cartera
    and in_pk.cod_especie = tesp.cod_especie
    and in_pk.cod_sis_origen = tesp.cod_sis_origen
    and in_pk.cod_disponibilidad = tesp.cod_disponibilidad
UNION ALL
-- perimetro de MMFF derivados
SELECT 'mmff - derivados swap' source
, concat_ws('_', trim(ds.cod_operacion), cast(ds.cod_pata as string)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, trim(ds.cod_operacion) mmff_cod_operacion
, cast(ds.cod_pata as string) mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from (
  select distinct b.cod_operacion, b.cod_pata
  from santander_business_risk_arda.derivados_swap b
  where b.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and b.fec_data <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and b.cod_cartera in ('ALC', 'INV')
  and b.cod_pata = 2
) ds
UNION ALL
SELECT 'mmff-derivados ndf' source
, concat_ws('_', trim(dn.cod_operacion)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, trim(dn.cod_operacion) mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from (
  select distinct b.cod_operacion
  from santander_business_risk_arda.derivados_ndf b
  where b.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and b.fec_data <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
  and b.cod_cartera in ('ALC', 'INV')
  and b.cod_pata = 2
) dn
union all
SELECT 'mmff-tactico' source
, concat_ws('_', trim(cuenta), trim(isin)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from bi_corp_staging.mmff_tactico_especie
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
union all
SELECT distinct 'mmff-tactico' source
, trim(especie) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from bi_corp_staging.mmff_tactico_especie
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
union all
SELECT distinct 'corresponsales-tactico' source
, concat_ws('_', trim(nup), trim(moneda), trim(rubro_altair)) id_cto_source
, '' cred_cod_entidad
, '' cred_cod_centro
, '' cred_num_cuenta
, '' cred_cod_producto
, '' cred_cod_subprodu_altair
, '' mmff_cod_especie
, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen
, '' mmff_cod_cartera
, '' mmff_cod_operacion
, '' mmff_cod_pata
, '' blc_sociedad
, '' blc_cargabal
, '' blc_sociedad_eliminacion
from bi_corp_staging.corresponsales 
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
) contratos;


CREATE TEMPORARY TABLE max_id_cto AS
SELECT COALESCE(MAX(cast(id_cto_bdr as BIGINT)), 0) as max_id_cto
FROM bi_corp_bdr.normalizacion_id_contratos_history;


INSERT INTO TABLE bi_corp_bdr.normalizacion_id_contratos_history
SELECT lpad(CAST(y.max_id_cto AS BIGINT) + z.rownum, 9, '0') as id_cto_bdr,
z.source,
z.id_cto_source,
z.cred_cod_entidad,
z.cred_cod_centro,
z.cred_num_cuenta,
z.cred_cod_producto,
z.cred_cod_subprodu_altair,
z.mmff_cod_especie,
z.mmff_cod_disponibilidad,
z.mmff_cod_sis_origen,
z.mmff_cod_cartera,
z.mmff_cod_operacion,
z.mmff_cod_pata,
z.blc_sociedad,
z.blc_cargabal,
z.blc_sociedad_eliminacion,
current_date as inserted_date
FROM (
SELECT x.*, ROW_NUMBER() OVER() AS rownum
FROM
(
SELECT p.*
FROM perimetro_contratos p
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history h ON h.id_cto_source = p.id_cto_source
WHERE h.id_cto_source is null
) x
) z, max_id_cto y;


INSERT OVERWRITE TABLE bi_corp_bdr.normalizacion_id_contratos
PARTITION ( partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' )
SELECT
hs.id_cto_bdr,
hs.source,
hs.id_cto_source,
hs.cred_cod_entidad,
hs.cred_cod_centro,
hs.cred_num_cuenta,
hs.cred_cod_producto,
hs.cred_cod_subprodu_altair,
hs.mmff_cod_especie,
hs.mmff_cod_disponibilidad,
hs.mmff_cod_sis_origen,
hs.mmff_cod_cartera,
hs.mmff_cod_operacion,
hs.mmff_cod_pata,
hs.blc_sociedad,
hs.blc_cargabal,
hs.blc_sociedad_eliminacion
FROM (
SELECT DISTINCT pc.id_cto_source FROM perimetro_contratos pc
) ds
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history hs ON hs.id_cto_source = ds.id_cto_source;