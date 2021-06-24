insert overwrite table bi_corp_nuevo_cargabal.master_oci_gross_tax_impact
select 'GRTX1','Gross','Bruto' union all
select 'GRTX2','Tax effect','Efecto fiscal' ;