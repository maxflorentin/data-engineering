hive -e "
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE productos_riesgos AS
select ma.cod_producto, p.desc_40
from santander_business_risk_arda.maestro_atributos ma, santander_business_risk_arda.producto p
where ma.data_date_part = '20190930'
and p.data_date_part = '20190930'
and p.cod_producto = ma.cod_producto
GROUP BY ma.cod_producto, p.desc_40;



CREATE TEMPORARY TABLE perimetro_contratos AS
SELECT DISTINCT contratos.* FROM (
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
WHERE a.data_date_part = '20190930'
AND NOT (a.cod_estado_rel_cliente = 'C' and a.fec_baja_rel_cliente <= '20190930' )
AND (a.cod_marca != 'FA' or a.cod_marca is null)

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
AND concat(substr(core.PEFECEST,1,4),substr(core.PEFECEST,6,2)) = substr('20190930',1,6)

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
WHERE a.data_date_part = '20190930' AND a.cod_marca = 'FA' AND substr(a.fec_castigo,1,7) = '2019-09'

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
WHERE partition_date = '2019-09'
GROUP BY cargabal, sociedad, sociedad_eliminacion) core

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.cod_entidadg, r.cod_centrog , r.num_contratg, r.cod_productg, r.cod_subprodg, r.cod_divisag) id_cto_source
, r.cod_entidadg as cred_cod_entidad
, r.cod_centrog  as cred_cod_centro
, r.num_contratg as cred_num_cuenta
, r.cod_productg as cred_cod_producto
, r.cod_subprodg as cred_cod_subprodu_altair
, r.cod_divisag  as cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata  
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_productg
WHERE r.partition_date = '2020-03-27' and r.fec_altareg between
'2019-09-01'
and
'2019-09-30'

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.mig_old_entidad, r.mig_old_cent_alta , concat('000', substring(r.mig_old_cuenta, 4, 12)), p.pecodpro, p.pecodsub,r.mig_old_divisa) id_cto_source
, r.mig_old_entidad cred_cod_entidad
, r.mig_old_cent_alta cred_cod_centro
, concat('000', substring(r.mig_old_cuenta, 4, 12)) cred_num_cuenta
, p.pecodpro cred_cod_producto
, p.pecodsub cred_cod_subprodu_altair
, r.mig_old_divisa cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p on p.pecodent = r.mig_old_entidad and p.pecodofi = r.mig_old_cent_alta and p.penumcon = concat('000', substring(r.mig_old_cuenta, 4, 12) ) and r.partition_date = p.partition_date
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
WHERE r.partition_date = '2020-03-27' and p.partition_date = '2020-03-27' and r.mig_old_fech_baja between
'2019-09-01'
and
'2019-09-30'

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.pecdgent, r.datos_comunes_peofiori , concat('000', substring(r.penumcon, 4)), r.pecodpro, r.pecodsub, r.pecodmon) id_cto_source
, r.pecdgent cred_cod_entidad
, r.datos_comunes_peofiori cred_cod_centro
, concat('000', substring(r.penumcon, 4)) cred_num_cuenta
, r.pecodpro cred_cod_producto
, r.pecodsub cred_cod_subprodu_altair
, r.pecodmon cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.pecodpro
WHERE r.partition_date = '2019-12-13' and r.pefecest between
'2019-09-01'
and
'2019-09-30'

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.cod_entidad, r.cod_centro, r.num_contrato, r.cod_producto, r.cod_subprodu, r.cod_divisa) id_cto_source
, r.cod_entidad cred_cod_entidad
, r.cod_centro cred_cod_centro
, r.num_contrato cred_num_cuenta
, r.cod_producto cred_cod_producto
, r.cod_subprodu cred_cod_subprodu_altair
, r.cod_divisa cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.mddtccn r
INNER JOIN productos_riesgos pr on pr.cod_producto = r.cod_producto
WHERE r.partition_date = '2020-03-27' and r.fec_altareg between
'2019-09-01'
and
'2019-09-30'

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.mig_new_entidad, r.mig_new_cent_alta, concat('000', substring(r.mig_new_cuenta, 4, 12)), p.pecodpro, p.pecodsub, r.mig_new_divisa) id_cto_source
, r.mig_new_entidad cred_cod_entidad
, r.mig_new_cent_alta cred_cod_centro
, concat('000', substring(r.mig_new_cuenta, 4, 12)) cred_num_cuenta
, p.pecodpro cred_cod_producto
, p.pecodsub cred_cod_subprodu_altair
, r.mig_new_divisa cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malbgc_zbdtmig r
LEFT JOIN bi_corp_staging.malpe_pedt008 p on p.pecodent = r.mig_old_entidad and p.pecodofi = r.mig_old_cent_alta and p.penumcon = concat('000', substring(r.mig_old_cuenta, 4, 12) ) and r.partition_date = p.partition_date
INNER JOIN productos_riesgos pr on pr.cod_producto = p.pecodpro
WHERE r.partition_date = '2020-03-27' and p.partition_date = '2020-03-27' and r.mig_old_fech_baja between
'2019-09-01'
and
'2019-09-30'

UNION ALL
SELECT
'credito' source
, concat_ws('_', r.pecdgentd, r.datos_comunes_peofides, concat('000', substring(r.penumcond, 4)), r.pecodprod, r.pecodsubd, r.pecodmond) id_cto_source
, r.pecdgentd cred_cod_entidad
, r.datos_comunes_peofides cred_cod_centro
, concat('000', substring(r.penumcond, 4)) cred_num_cuenta
, r.pecodprod cred_cod_producto
, r.pecodsubd cred_cod_subprodu_altair
, r.pecodmond cred_cod_divisa
, '' mmff_cod_especie, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion, '' mmff_cod_pata
, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion
FROM bi_corp_staging.malpe_peec867c r
INNER JOIN productos_riesgos p on p.cod_producto = r.pecodprod
WHERE r.partition_date = '2019-12-13' and r.pefecest between
'2019-09-01'
and
'2019-09-30'
) contratos;


CREATE TEMPORARY TABLE max_id_cto AS
SELECT COALESCE(MAX(id_cto_bdr),0) as max_id_cto
FROM bi_corp_bdr.normalizacion_id_contratos_history_max;

INSERT INTO TABLE bi_corp_bdr.normalizacion_id_contratos_history_max
SELECT CAST(y.max_id_cto AS INT) + z.rownum as id_cto_bdr,
z.source,
z.id_cto_source,
z.cred_cod_entidad,
z.cred_cod_centro,
z.cred_num_cuenta,
z.cred_cod_producto,
z.cred_cod_subprodu_altair,
z.cred_cod_divisa,
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
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history_max h ON h.id_cto_source = p.id_cto_source
WHERE h.id_cto_source is null
) x
) z, max_id_cto y;

INSERT OVERWRITE TABLE bi_corp_bdr.normalizacion_id_contratos_test
PARTITION ( partition_date = '2019-09' )
SELECT
hs.id_cto_bdr,
hs.source,
hs.id_cto_source,
hs.cred_cod_entidad,
hs.cred_cod_centro,
hs.cred_num_cuenta,
hs.cred_cod_producto,
hs.cred_cod_subprodu_altair,
hs.cred_cod_divisa,
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
LEFT JOIN bi_corp_bdr.normalizacion_id_contratos_history_max hs ON hs.id_cto_source = ds.id_cto_source;
"