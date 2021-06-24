set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_relacionclientesciti
PARTITION(partition_date)

SELECT CAST(nup AS BIGINT) cod_per_nup
    , trim(ident_otro_bco) cod_cys_identorigen
    , trim(entidad_origen) cod_cys_entidadorigen
    , partition_date
FROM bi_corp_staging.rio125_rel_nup_otro_bco ;

