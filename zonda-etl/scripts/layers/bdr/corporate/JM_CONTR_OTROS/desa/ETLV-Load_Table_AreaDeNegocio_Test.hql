set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--Area de Negocio
--Luego de cargar en contratos otros datos el Area de negocio GLOBAL de Rosetta (E0623_UNNTS) 
--queda un remanente de contratos con un valor por defecto '00000', esto se pretende cambiar 
--por otro valor por defecto hallado por la siguiente lógica.
with intervinientes_contrato AS (
		select g4128_contra1 as contra1, g4128_tipintev as tipintev, g4128_idnumcli as idnumcli                                      
		from bi_corp_bdr.jm_interv_cto
		where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' 
         and g4128_tipintev = '00001'
	),
	mapeo as (
		select 'IU' as codigo_de_cuadrante, '00146' as BG union all
		select 'F2' as codigo_de_cuadrante, '00087' as BG union all
		select 'OF' as codigo_de_cuadrante, '00087' as BG union all
		select 'IP' as codigo_de_cuadrante, '00146' as BG union all
		select 'C1' as codigo_de_cuadrante, '00146' as BG union all
		select 'S'  as codigo_de_cuadrante, '00144' as BG union all
		select 'A'  as codigo_de_cuadrante, '00144' as BG union all
		select 'B'  as codigo_de_cuadrante, '00144' as BG union all
		select 'C'  as codigo_de_cuadrante, '00144' as BG union all
		select 'P1' as codigo_de_cuadrante, '00146' as BG union all
		select 'P2' as codigo_de_cuadrante, '00146' as BG union all
		select 'EM' as codigo_de_cuadrante, '00146' as BG union all
		select 'G1' as codigo_de_cuadrante, '00146' as BG union all
		select 'GL' as codigo_de_cuadrante, '00087' as BG union all
		select 'LA' as codigo_de_cuadrante, '00087' as BG union all
		select 'LO' as codigo_de_cuadrante, '00087' as BG union all
		select 'MU' as codigo_de_cuadrante, '00087' as BG union all
		select 'PU' as codigo_de_cuadrante, '00087' as BG union all
		select 'OT' as codigo_de_cuadrante, '00087' as BG union all
		select 'FI' as codigo_de_cuadrante, '00087' as BG union all
		select 'F1' as codigo_de_cuadrante, '00087' as BG 
	),
	contratos_con_bg_con_repetidos as (
		select row_number() over(partition by ic.contra1 order by ic.idnumcli asc) as orden, ic.contra1 as contra1, m.bg as bg
		from intervinientes_contrato ic
    		left join bi_corp_bdr.jm_client_bii bii
			   on ic.idnumcli = bii.g4093_idnumcli 
			   and bii.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
			left join mapeo m
			   on bii.g4093_tipsegl2 = rpad(m.codigo_de_cuadrante,40,' ')
		where m.bg is not null
	),
	contratos_con_bg as (
		select cr.contra1, cr.bg
		from contratos_con_bg_con_repetidos cr
		where orden = 1
	)
insert overwrite table bi_corp_bdr.test_jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}')
select c.e0623_feoperac, c.e0623_s1emp, c.e0623_contra1, c.e0623_fec_mes,
       c.e0623_vcto_res, c.e0623_vcto_pon, c.e0623_idsubprd, c.e0623_tip_liqu,
       c.e0623_liq_pzo,  c.e0623_tip_amor, c.e0623_amor_pzo, c.e0623_amor_sis,
       c.e0623_ctb_situ, c.e0623_gest_sit, c.e0623_ges2_sit, c.e0623_ataemax,
       c.e0623_tip_int,  c.e0623_imp1limi, c.e0623_alimact, c.e0623_importh,
       c.e0623_inv_nego, c.e0623_iprovisi, c.e0623_iprovis1, c.e0623_fecultmo,
       c.e0623_estadtrj, c.e0623_inactrj, c.e0623_unnt,
	    case when (c.e0623_unnts != '00000') then c.e0623_unnts
		      when (c.e0623_unnts = '00000' and bg is not null) then bg
		      else '00000' end as e0623_unnts,
       c.e0623_unnv, c.e0623_unnvs, c.e0623_rgosub, c.e0623_fecincar,
       c.e0623_fecficar, c.e0623_intneg, c.e0623_mtvalta, c.e0623_indsubro,
       c.e0623_tip_inte, c.e0623_difernci, c.e0623_imprtcuo, c.e0623_indsegur,
       c.e0623_amortpar, c.e0623_fecimpag, c.e0623_impprimp, c.e0623_fhprimpg,
       c.e0623_impimpnr, c.e0623_estprinm, c.e0623_exclcto, c.e0623_cuotimpa,
       c.e0623_limocult, c.e0623_codimphi, c.e0623_indeucon, c.e0623_tipcaren,
       c.e0623_cuotpres, c.e0623_ibuysell, c.e0623_sutipint, c.e0623_tetipint,
       c.e0623_tipsuelo, c.e0623_tiptecho, c.e0623_plrevtin, c.e0623_feccuota,
       c.e0623_nudiaatr, c.e0623_segpllim, c.e0623_voltrans, c.e0623_marcauti,
       c.e0623_topdeale, c.e0623_fecrepre, c.e0623_emprepor, c.e0623_impcuimp,
       c.e0623_tipinefe, c.e0623_forpgact, c.e0623_finiutct, c.e0623_ffinutct
from bi_corp_bdr.test_jm_contr_otros c 
   left join contratos_con_bg bg
      on bg.contra1 = c.e0623_contra1
where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'