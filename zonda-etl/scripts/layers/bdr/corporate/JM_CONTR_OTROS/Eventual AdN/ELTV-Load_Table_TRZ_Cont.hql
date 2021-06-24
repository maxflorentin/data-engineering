set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_bdr.temp_jm_contr_otros 
select 
    e0623_feoperac
    ,e0623_s1emp
    ,e0623_contra1
    ,e0623_fec_mes
    ,e0623_vcto_res
    ,e0623_vcto_pon
    ,e0623_idsubprd
    ,e0623_tip_liqu
    ,e0623_liq_pzo
    ,e0623_tip_amor
    ,e0623_amor_pzo
    ,e0623_amor_sis
    ,e0623_ctb_situ
    ,e0623_gest_sit
    ,e0623_ges2_sit
    ,e0623_ataemax
    ,e0623_tip_int
    ,e0623_imp1limi
    ,e0623_alimact
    ,e0623_importh
    ,e0623_inv_nego
    ,e0623_iprovisi
    ,e0623_iprovis1
    ,e0623_fecultmo
    ,e0623_estadtrj
    ,e0623_inactrj
    ,e0623_unnt
    ,e0623_unnts
    ,e0623_unnv
    ,e0623_unnvs
    ,e0623_rgosub
    ,g7025_fealtrel as e0623_fecincar
    ,g7025_fec_baja as e0623_fecficar
    ,e0623_intneg
    ,e0623_mtvalta
    ,e0623_indsubro
    ,e0623_tip_inte
    ,e0623_difernci
    ,e0623_imprtcuo
    ,e0623_indsegur
    ,e0623_amortpar
    ,e0623_fecimpag
    ,e0623_impprimp
    ,e0623_fhprimpg
    ,e0623_impimpnr
    ,e0623_estprinm
    ,e0623_exclcto
    ,e0623_cuotimpa
    ,e0623_limocult
    ,e0623_codimphi
    ,e0623_indeucon
    ,'00002' as e0623_tipcaren
    ,e0623_cuotpres
    ,e0623_ibuysell
    ,e0623_sutipint
    ,e0623_tetipint
    ,e0623_tipsuelo
    ,e0623_tiptecho
    ,e0623_plrevtin
    ,e0623_feccuota
    ,e0623_nudiaatr
    ,e0623_segpllim
    ,e0623_voltrans
    ,e0623_marcauti
    ,e0623_topdeale
    ,e0623_fecrepre
    ,e0623_emprepor
    ,e0623_impcuimp
    ,e0623_tipinefe
    ,e0623_forpgact
    ,e0623_finiutct
    ,e0623_ffinutct
 from 
       (
           select g7025_s1emp, g7025_contra1, g7025_emp_ant, g7025_cont_ant, min(g7025_fealtrel) as g7025_fealtrel, 
                  g7025_fec_mod, g7025_coddiv,  min(g7025_fec_baja) as g7025_fec_baja
            from bi_corp_bdr.jm_trz_cont_ren 
            where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}'
            and g7025_motrenug in ('00026','00029','00030') --Corresponden a la moratoria covid
            group by g7025_s1emp, g7025_contra1, g7025_emp_ant, g7025_cont_ant, g7025_fec_mod,
                     g7025_coddiv
        ) trz  
        inner join 
        (
            select *
            from bi_corp_bdr.jm_contr_otros 
            where partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}'
        ) ctr
        on trz.G7025_CONTRA1 = ctr.E0623_CONTRA1;
               
INSERT INTO TABLE bi_corp_bdr.temp_jm_contr_otros       
select 
        ctr.e0623_feoperac
        ,ctr.e0623_s1emp
        ,ctr.e0623_contra1
        ,ctr.e0623_fec_mes
        ,ctr.e0623_vcto_res
        ,ctr.e0623_vcto_pon
        ,ctr.e0623_idsubprd
        ,ctr.e0623_tip_liqu
        ,ctr.e0623_liq_pzo
        ,ctr.e0623_tip_amor
        ,ctr.e0623_amor_pzo
        ,ctr.e0623_amor_sis
        ,ctr.e0623_ctb_situ
        ,ctr.e0623_gest_sit
        ,ctr.e0623_ges2_sit
        ,ctr.e0623_ataemax
        ,ctr.e0623_tip_int
        ,ctr.e0623_imp1limi
        ,ctr.e0623_alimact
        ,ctr.e0623_importh
        ,ctr.e0623_inv_nego
        ,ctr.e0623_iprovisi
        ,ctr.e0623_iprovis1
        ,ctr.e0623_fecultmo
        ,ctr.e0623_estadtrj
        ,ctr.e0623_inactrj
        ,ctr.e0623_unnt
        ,ctr.e0623_unnts
        ,ctr.e0623_unnv
        ,ctr.e0623_unnvs
        ,ctr.e0623_rgosub
        ,ctr.e0623_fecincar
        ,ctr.e0623_fecficar
        ,ctr.e0623_intneg
        ,ctr.e0623_mtvalta
        ,ctr.e0623_indsubro
        ,ctr.e0623_tip_inte
        ,ctr.e0623_difernci
        ,ctr.e0623_imprtcuo
        ,ctr.e0623_indsegur
        ,ctr.e0623_amortpar
        ,ctr.e0623_fecimpag
        ,ctr.e0623_impprimp
        ,ctr.e0623_fhprimpg
        ,ctr.e0623_impimpnr
        ,ctr.e0623_estprinm
        ,ctr.e0623_exclcto
        ,ctr.e0623_cuotimpa
        ,ctr.e0623_limocult
        ,ctr.e0623_codimphi
        ,ctr.e0623_indeucon
        ,ctr.e0623_tipcaren
        ,ctr.e0623_cuotpres
        ,ctr.e0623_ibuysell
        ,ctr.e0623_sutipint
        ,ctr.e0623_tetipint
        ,ctr.e0623_tipsuelo
        ,ctr.e0623_tiptecho
        ,ctr.e0623_plrevtin
        ,ctr.e0623_feccuota
        ,ctr.e0623_nudiaatr
        ,ctr.e0623_segpllim
        ,ctr.e0623_voltrans
        ,ctr.e0623_marcauti
        ,ctr.e0623_topdeale
        ,ctr.e0623_fecrepre
        ,ctr.e0623_emprepor
        ,ctr.e0623_impcuimp
        ,ctr.e0623_tipinefe
        ,ctr.e0623_forpgact
        ,ctr.e0623_finiutct
        ,ctr.e0623_ffinutct
        from bi_corp_bdr.jm_contr_otros ctr
                left join bi_corp_bdr.temp_jm_contr_otros tco
                       on tco.e0623_contra1 = ctr.e0623_contra1 
        where ctr.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}'
             and tco.e0623_contra1 is null; 
                                
insert overwrite table bi_corp_bdr.jm_contr_otros 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}')
select 
        e0623_feoperac
        ,e0623_s1emp
        ,e0623_contra1
        ,e0623_fec_mes
        ,e0623_vcto_res
        ,e0623_vcto_pon
        ,e0623_idsubprd
        ,e0623_tip_liqu
        ,e0623_liq_pzo
        ,e0623_tip_amor
        ,e0623_amor_pzo
        ,e0623_amor_sis
        ,e0623_ctb_situ
        ,e0623_gest_sit
        ,e0623_ges2_sit
        ,e0623_ataemax
        ,e0623_tip_int
        ,e0623_imp1limi
        ,e0623_alimact
        ,e0623_importh
        ,e0623_inv_nego
        ,e0623_iprovisi
        ,e0623_iprovis1
        ,e0623_fecultmo
        ,e0623_estadtrj
        ,e0623_inactrj
        ,e0623_unnt
        ,e0623_unnts
        ,e0623_unnv
        ,e0623_unnvs
        ,e0623_rgosub
        ,e0623_fecincar
        ,e0623_fecficar
        ,e0623_intneg
        ,e0623_mtvalta
        ,e0623_indsubro
        ,e0623_tip_inte
        ,e0623_difernci
        ,e0623_imprtcuo
        ,e0623_indsegur
        ,e0623_amortpar
        ,e0623_fecimpag
        ,e0623_impprimp
        ,e0623_fhprimpg
        ,e0623_impimpnr
        ,e0623_estprinm
        ,e0623_exclcto
        ,e0623_cuotimpa
        ,e0623_limocult
        ,e0623_codimphi
        ,e0623_indeucon
        ,e0623_tipcaren
        ,e0623_cuotpres
        ,e0623_ibuysell
        ,e0623_sutipint
        ,e0623_tetipint
        ,e0623_tipsuelo
        ,e0623_tiptecho
        ,e0623_plrevtin
        ,e0623_feccuota
        ,e0623_nudiaatr
        ,e0623_segpllim
        ,e0623_voltrans
        ,e0623_marcauti
        ,e0623_topdeale
        ,e0623_fecrepre
        ,e0623_emprepor
        ,e0623_impcuimp
        ,e0623_tipinefe
        ,e0623_forpgact
        ,e0623_finiutct
        ,e0623_ffinutct
from  bi_corp_bdr.temp_jm_contr_otros;