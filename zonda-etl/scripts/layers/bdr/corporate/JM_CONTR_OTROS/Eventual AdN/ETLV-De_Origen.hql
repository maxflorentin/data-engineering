set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.contr_otros_area_negocio
    (
        id_cto_bdr STRING,
        unnv       STRING,
        unnts      STRING
    );

with
 tabla as (
               select * from (
                              select row_number() over(partition by c.id_cto_bdr,rn.segmentation_code order by rn.global_value asc) as orden,
                                     c.id_cto_bdr , rn.segmentation_code , rn.global_value
                                from bi_corp_bdr.perim_contratos_bis c
                              inner join bi_corp_common.rosetta_nkey_hist nk
                                  on c.id_cto_bdr = nk.native_key
                                 and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}'
                                 and nk.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_nkey_hist', dag_id='BDR_LOAD_Area_De_Negocio') }}'
                                 and nk.domain_code ='00004'
                              inner join bi_corp_common.rosetta_nkey_hist nk2
                                  on nk2.master_key = nk.master_key
                                 and nk2.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_nkey_hist', dag_id='BDR_LOAD_Area_De_Negocio') }}'
                                 and nk2.domain_code = '00005'
                              inner join bi_corp_common.rosetta_rnkd_hist rn
                                  on rn.native_key = nk2.native_key
                                 and rn.partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rosetta_rnkd_hist', dag_id='BDR_LOAD_Area_De_Negocio') }}'
                                 and rn.domain_code = '00005'
                                 and rn.global_value != 'AN04020000'
                               where rn.segmentation_code in ( '00001', '00004')
                                 and trim(rn.global_value) != '998'
                              order by c.id_cto_bdr
                              ) a
                where orden = 1
            ),
 baremos as (
             select t.id_cto_bdr, t.segmentation_code, bl.cod_baremo_local, bgl.cod_baremo_global
               from tabla t
             left join bi_corp_bdr.baremos_local bl
                 on bl.cod_negocio_local in ('86','116')
                and t.global_value = bl.cod_baremo_alfanumerico_local
             left join bi_corp_bdr.map_baremos_global_local bgl
                 on bl.cod_negocio_local = cast(bgl.cod_negocio_local as string)
                and bl.cod_baremo_local = bgl.cod_baremo_local
             )
insert overwrite table bi_corp_bdr.contr_otros_area_negocio
select b1.id_cto_bdr,
       lpad(b4.cod_baremo_local,5,'0'),
       lpad(b1.cod_baremo_global,5,'0')
from baremos b1 inner join baremos b4
	on b1.id_cto_bdr = b4.id_cto_bdr
	    and b1.segmentation_code = '00001'
	    and b4.segmentation_code = '00004';


insert overwrite table bi_corp_bdr.jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}')

SELECT DISTINCT
E0623_FEOPERAC,
E0623_S1EMP,
E0623_CONTRA1,
E0623_FEC_MES,
E0623_VCTO_RES,
E0623_VCTO_PON,
E0623_IDSUBPRD,
E0623_TIP_LIQU,
E0623_LIQ_PZO,
E0623_TIP_AMOR,
E0623_AMOR_PZO,
E0623_AMOR_SIS,
E0623_CTB_SITU,
E0623_GEST_SIT,
E0623_GES2_SIT,
E0623_ATAEMAX,
E0623_TIP_INT,
E0623_IMP1LIMI,
E0623_ALIMACT,
E0623_IMPORTH,
E0623_INV_NEGO,
E0623_IPROVISI,
E0623_IPROVIS1,
E0623_FECULTMO,
E0623_ESTADTRJ,
E0623_INACTRJ,
E0623_UNNT,
lpad(nvl(an.unnts,'0'), 5, '0') AS E0623_UNNTS,
lpad(nvl(an.unnv,'0'), 5, '0') AS E0623_UNNV,
E0623_UNNVS,
E0623_RGOSUB,
E0623_FECINCAR,
E0623_FECFICAR,
E0623_INTNEG,
E0623_MTVALTA,
E0623_INDSUBRO,
E0623_TIP_INTE,
E0623_DIFERNCI,
E0623_IMPRTCUO,
E0623_INDSEGUR,
E0623_AMORTPAR,
E0623_FECIMPAG,
E0623_IMPPRIMP,
E0623_FHPRIMPG,
E0623_IMPIMPNR,
E0623_ESTPRINM,
E0623_EXCLCTO,
E0623_CUOTIMPA,
E0623_LIMOCULT,
E0623_CODIMPHI,
E0623_INDEUCON,
E0623_TIPCAREN,
E0623_CUOTPRES,
E0623_IBUYSELL,
E0623_SUTIPINT,
E0623_TETIPINT,
E0623_TIPSUELO,
E0623_TIPTECHO,
E0623_PLREVTIN,
E0623_FECCUOTA,
E0623_NUDIAATR,
E0623_SEGPLLIM,
E0623_VOLTRANS,
E0623_MARCAUTI,
E0623_TOPDEALE,
E0623_FECREPRE,
E0623_EMPREPOR,
E0623_IMPCUIMP,
E0623_TIPINEFE,
E0623_FORPGACT,
E0623_FINIUTCT,
E0623_FFINUTCT

FROM bi_corp_bdr.jm_contr_otros a

LEFT JOIN bi_corp_bdr.contr_otros_area_negocio an
            ON an.id_cto_bdr = a.E0623_CONTRA1

WHERE a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Area_De_Negocio') }}'
;