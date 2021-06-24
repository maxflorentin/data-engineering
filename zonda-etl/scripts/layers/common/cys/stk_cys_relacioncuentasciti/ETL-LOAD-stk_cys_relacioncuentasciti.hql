set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_relacioncuentasciti
PARTITION(partition_date)

SELECT trim(entidad) cod_cys_entidad
    , CAST(centro AS INT) cod_cys_centro
    , CAST(contrato AS INT) cod_cys_contrato
    , trim(producto) cod_cys_producto
    , trim(subproducto) cod_cys_subproducto
    , trim(divisa) cod_cys_divisa
    , CAST(centro_otro_bco AS INT) cod_cys_centroorigen
    , CAST(contrato_otro_bco AS INT) cod_cys_contratoorigen
    , trim(entidad_origen) cod_cys_entidadorigen
    , partition_date
FROM bi_corp_staging.rio125_rel_contrato_otro_bco ;