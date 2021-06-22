set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_bdr.normalizacion_id_contratos  
PARTITION (partition_date = '2019-09')

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
WHERE a.data_date_part = '20190930' 
AND NOT (a.cod_producto = '70' and a.cod_estado = '30') 
AND NOT (a.cod_estado_rel_cliente = 'C' and a.fec_baja_rel_cliente <= '20191130') 
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
WHERE core.partition_date = '2019-12-04' 
AND core.PEESTOPE = 'C' 
AND concat(substr(core.PEFECEST,1,4),substr(core.PEFECEST,6,2)) = substr('20190930',1,6)  
UNION ALL 
SELECT 'balance sociedad' source 
, concat_ws('_', core.sociedad, core.cargabal, core.sociedad_eliminacion) id_cto_source 
, '' cred_cod_entidad, '' cred_cod_centro, '' cred_num_cuenta, '' cred_cod_producto
, '' cred_cod_subprodu_altair , '' cred_cod_divisa, '' mmff_cod_especie, '' mmff_cod_disponibilidad
, '' mmff_cod_sis_origen, '' mmff_cod_cartera 
, '' mmff_cod_operacion, '' mmff_cod_pata, core.sociedad blc_sociedad, core.cargabal blc_cargabal 
, core.sociedad_eliminacion blc_sociedad_eliminacion 
FROM ( 
SELECT cargabal, sociedad, sociedad_eliminacion FROM bi_corp_bdr.balances_ajustes 
WHERE partition_date = '2019-09' 
GROUP BY cargabal, sociedad, sociedad_eliminacion) core 
UNION ALL 
SELECT 'credito' source 
, concat_ws ('_', r.cod_entidad, r.cod_centrod, r.num_contratd, r.cod_productd, r.cod_subprodd, r.cod_divisad) id_cto_source 
, r.cod_entidad cred_cod_entidad 
, r.cod_centrod cred_cod_centro 
, r.num_contratd cred_num_cuenta 
, r.cod_productd cred_cod_producto 
, r.cod_subprodd cred_cod_subprodu_altair , r.cod_divisad cred_cod_divisa
, '' mmff_cod_especie 
, '' mmff_cod_disponibilidad, '' mmff_cod_sis_origen, '' mmff_cod_cartera, '' mmff_cod_operacion 
, '' mmff_cod_pata, '' blc_sociedad, '' blc_cargabal, '' blc_sociedad_eliminacion 
FROM santander_business_risk_arda.contratos_refinanciados r  
INNER JOIN (
SELECT cod_entidad
, cod_centro, num_cuenta
, cod_producto, cod_subprodu 
, cod_divisa, cod_centrod, num_contratd, cod_productd, cod_subprodd,cod_divisad
, max(data_date_part) as max_data_date_part 
FROM santander_business_risk_arda.contratos_refinanciados 
WHERE substr(data_date_part,1,6) = '201909' 
AND substr(fec_refinan,1,7) = '2019-09' 
GROUP BY cod_entidad,cod_centro,num_cuenta, cod_producto
, cod_subprodu, cod_divisa, cod_centrod , num_contratd, cod_productd, cod_subprodd, cod_divisad ) 
PIV ON r.cod_entidad = piv.cod_entidad AND r.cod_centro = piv.cod_centro 
AND r.num_cuenta = piv.num_cuenta AND r.cod_producto = piv.cod_producto 
AND r.cod_subprodu = piv.cod_subprodu AND r.cod_divisa = piv.cod_divisa 
AND r.cod_centrod = piv.cod_centrod AND r.num_contratd = piv.num_contratd 
AND r.cod_productd = piv.cod_productd AND r.cod_subprodd = piv.cod_subprodd 
AND r.cod_divisad = piv.cod_divisad AND r.data_date_part = piv.max_data_date_part 
)cto )dist 

LEFT JOIN bi_corp_bdr.normalizacion_id_contratos  nor 
ON nor.id_cto_source = dist.id_cto_source 
AND nor.partition_date <= '2019-11';
