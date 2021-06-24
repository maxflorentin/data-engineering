SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

with dim_segmentos as (
   select *,
   CASE WHEN flag_per_mrg=1 THEN "MODELO DE RELACION GLOBAL"
   		ELSE NULL END ds_per_mrg,
   CASE WHEN flag_per_bpr=1 THEN "BANCA PRIVADA"
    	ELSE NULL END ds_per_bpr,
   CASE WHEN flag_per_gex=1 THEN "INTERNATIONAL DESK"
       	ELSE NULL END ds_per_gex,
   CASE WHEN flag_per_pst=1 THEN "PASAPORTE SANTANDER"
       	ELSE NULL END ds_per_pst,
   CASE WHEN flag_per_fig=1 THEN "INSTITUCIONES FINANC Y CNIAS DE SEGUROS"
       	ELSE NULL END ds_per_fig,
   CASE WHEN flag_per_uge=1 THEN "UNIDAD DE GRANDES EMPRESAS"
       	ELSE NULL END ds_per_uge,
   CASE WHEN flag_per_pba=1 THEN "PROGRAMA DE BANCARIZACION"
       	ELSE NULL END ds_per_pba,
   CASE WHEN flag_per_cit=1 THEN  "CLIENTE INFORMADO EN COMPRA DEL CITI (SEA NUP NUEVO O NUP EXISTENTE EN BSR)"
   		ELSE NULL END ds_per_cit
   from
   bi_corp_common.dim_per_segmentos_b)

INSERT OVERWRITE TABLE bi_corp_common.dim_per_segmentos
SELECT
    cod_per_segmentoduro      ,
    ds_per_segmento           ,
    ds_per_subsegmento        ,
    cod_per_cuadrante         ,
    cod_per_tipopersona       ,
    flag_per_MRG                  ,
    flag_per_BPR                  ,
    flag_per_CIT                  ,
    flag_per_GEX                  ,
    flag_per_PST                  ,
    flag_per_FIG                  ,
    flag_per_UGE                  ,
    flag_per_PBA                  ,
    flag_per_BPW                  ,
    ds_per_grupo                  ,
	concat_ws('+',ds_per_mrg, ds_per_bpr, ds_per_gex, ds_per_pst, ds_per_fig, ds_per_uge, ds_per_pba, ds_per_cit) val
   from dim_segmentos;