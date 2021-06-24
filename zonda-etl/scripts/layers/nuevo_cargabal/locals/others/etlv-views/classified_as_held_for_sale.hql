insert overwrite table bi_corp_nuevo_cargabal.master_classified_as_held_for_sale
select 'HFS1','Classified as held for sale','Mantenidos para la venta' union all
select 'HFS2','Other than classified as held for sale','Distintos a los mantenidos para la venta';