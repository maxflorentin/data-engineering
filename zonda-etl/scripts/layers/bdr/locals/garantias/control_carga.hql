INVALIDATE METADATA bi_corp_bdr.aux_garant_contratos_div;
INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_especificas_div;
INVALIDATE METADATA bi_corp_bdr.aux_garant_calif_pais;
INVALIDATE METADATA bi_corp_bdr.aux_garant_calif_empresa;
INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_genericas;
INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_contratos_div;	

INVALIDATE METADATA bi_corp_bdr.jm_contr_bis;
INVALIDATE METADATA bi_corp_bdr.normalizacion_id_contratos;
INVALIDATE METADATA bi_corp_bdr.jm_posic_contr;
INVALIDATE METADATA bi_corp_bdr.jm_interv_cto;

INVALIDATE METADATA bi_corp_bdr.jm_garant_cto;
INVALIDATE METADATA bi_corp_bdr.JM_GARA_REAL;
  

SELECT count(1) as cant  from bi_corp_bdr.aux_garant_calif_pais where partition_date = '${fecha_proceso}';

SELECT count(1) as cant  from bi_corp_bdr.aux_garant_calif_empresa where partition_date = '${fecha_proceso}' limit 10;


SELECT * FROM (
SELECT 1 as orden,'aux_garant_contratos_div' as tabla, '${partition_date}' as periodo, count(1) as cant  from bi_corp_bdr.aux_garant_contratos_div where partition_date =  '${partition_date}'
UNION
SELECT 2 as orden,'aux_garant_garantias_genericas' as tabla, '${partition_date}' as periodo, count(1) as cant  from bi_corp_bdr.aux_garant_garantias_genericas where partition_date =  '${partition_date}' 
UNION
SELECT 3 as orden,'aux_garant_garantias_especificas_div' as tabla, '${partition_date}' as periodo, count(1) as cant  from bi_corp_bdr.aux_garant_garantias_especificas_div where partition_date =  '${partition_date}'
UNION
SELECT 4 as orden,'aux_garant_garantias_contratos_div' as tabla, '${partition_date}' as periodo, count(1) as cant  from bi_corp_bdr.aux_garant_garantias_contratos_div where partition_date =  '${partition_date}'
UNION
SELECT 5 as orden,'jm_garant_cto' as tabla, from_timestamp('${partition_date}', 'yyyy-MM') as periodo, count(1) as cant  from bi_corp_bdr.jm_garant_cto where partition_date = from_timestamp('${partition_date}', 'yyyy-MM')
UNION
SELECT 6 as orden,'jm_gara_real' as tabla, from_timestamp('${partition_date}', 'yyyy-MM') as periodo, count(1) as cant  from bi_corp_bdr.jm_gara_real where partition_date = from_timestamp('${partition_date}', 'yyyy-MM')
)t order by orden;

