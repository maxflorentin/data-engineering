INSERT OVERWRITE TABLE bi_corp_bdr.mp_02_mddtccn partition(partition_date)
select
cod_entidadg,
cod_centrog,
num_contratg,
cod_productg,
cod_subprodg,
cod_entidad,
cod_centro,
num_contrato,
cod_producto,
cod_subprodu,
fec_altareg,
cod_divisa,
fec_baja,
partition_date
FROM bi_corp_staging.mddtccn
WHERE partition_date = '2021-05-31'
AND fec_altareg BETWEEN '2021-05-01' AND '2021-05-31'
;