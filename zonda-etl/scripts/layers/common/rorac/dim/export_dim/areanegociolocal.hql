"
set mapred.job.queue.name=root.dataeng ;

SELECT cod_ren_areanegociohijo
 		, ds_ren_areanegociohijo
 FROM bi_corp_common.dim_ren_jeareanegocioctr 
 WHERE cod_ren_indcorporativo = 'C'
 ORDER BY cod_ren_areanegociohijo ;
"