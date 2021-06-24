set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PRODPLAN as
select  CAST(CAPB.capb_carp_cd_ramo AS BIGINT) cod_seg_ramo,
        TRIM(CAPB.capb_capu_cd_producto) cod_seg_producto,
        TRIM(CAPU.capu_de_producto_larga) ds_seg_producto,
        TRIM(CAPB.capb_cd_plan) cod_seg_plan,
        trim(CAPB.capb_de_plan) ds_seg_plan,
        TRIM(case
            when (TRIM(CAPB.capb_carp_cd_ramo) = '1') then 'APE'
            when (TRIM(CAPB.capb_carp_cd_ramo) = '8') then 'INC'
            when (TRIM(CAPB.capb_carp_cd_ramo) in ('16','19')) then
                case
                    when (TRIM(CAPB.capb_cd_clasificacion) = 'PLT') then 'RCB'
                    when (TRIM(CAPB.capb_cd_clasificacion) = 'BLK') then 'RCB'
                    when (case when TRIM(CAPB.capb_cd_clasificacion) IS NULL OR TRIM(CAPB.capb_cd_clasificacion) ='null' then 'IND' else TRIM(CAPB.capb_cd_clasificacion) end) not in ('PLT','BLK') then 'RCJ'
                end
            when (trim(capb_carp_cd_ramo) in ('25','52')) then
                case
                    when (TRIM(CAPU.capu_auus_cd_uso)  in ('C','01')) then 'PCA'
                    when (TRIM(CAPU.capu_auus_cd_uso)  = 'M') then 'PMO'
                    when (TRIM(CAPU.capu_auus_cd_uso)  = 'O') then 'PIN'
                end
            when (TRIM(capb_carp_cd_ramo) = '75') then
                case
                    when (TRIM(CAPB.capb_cd_categoria) = 'MO') then 'PMO'
                    when (TRIM(CAPB.capb_cd_categoria) = 'SA') then 'SAL'
                    when (TRIM(CAPB.capb_cd_categoria) = 'VI') then 'VID'
                    when (TRIM(CAPB.capb_cd_categoria) = 'AP') then 'APE'
                    when (TRIM(CAPB.capb_cd_categoria) = 'HO') then 'HOG'
                    when (TRIM(CAPB.capb_cd_categoria) = 'AU') then 'AUT'
                end
            when ((TRIM(CAPB.capb_carp_cd_ramo) in ('20','58')) or ((TRIM(CAPB.capb_carp_cd_ramo) = '18') and (TRIM(CAPB.capb_cd_clasificacion) = 'AUT'))) then
                case
                    when (TRIM(CAPU.capu_cacy_cd_clase) = 'IP') then 'IPR'
                    when (TRIM(CAPU.capu_cacy_cd_clase) = 'PP') then 'PPA'
                end
            when ((TRIM(CAPB.capb_carp_cd_ramo) in ('22','23')) or (cast(CAPB.capb_carp_cd_ramo as bigint) between 60 and 74)) then
                case
                    when (TRIM(CAPB.capb_carp_cd_ramo) in ('60','22')) then 'COM'
                    when (TRIM(CAPB.capb_carp_cd_ramo) in ('62','23')) then 'CON'
                    when (TRIM(CAPB.capb_carp_cd_ramo) = '66') then 'ART'
                    else NULL
                end
            when (TRIM(CAPB.capb_carp_cd_ramo) in ('24','59')) then
                case
                    when ((TRIM(CAPU.capu_auus_cd_uso) = 'F') or (TRIM(CAPB.capb_cd_categoria) = 'FE')) then 'PFE'
                    when ((TRIM(CAPU.capu_auus_cd_uso) = 'M') or (TRIM(CAPB.capb_cd_categoria) = 'MA')) then 'PMA'
                    when (TRIM(CAPB.capb_cd_categoria) = 'SA') then 'SAL'
                    else 'SAL'
                end
            when (TRIM(CAPB.capb_carp_cd_ramo) = '21') then 'HOG'
            when (TRIM(CAPB.capb_carp_cd_ramo) in ('26','53')) then 'GPR'
            when ((TRIM(CAPB.capb_carp_cd_ramo) = '18') and (TRIM(CAPU.capu_cacy_cd_clase) not in ('IP','PP'))) then 'VID'
            when (TRIM(CAPB.capb_carp_cd_ramo) = '45') then 'INC'
            when (TRIM(CAPB.capb_carp_cd_ramo) = '48') then 'VID'
            when (TRIM(CAPB.capb_carp_cd_ramo) = '49') then 'VID'
            when (TRIM(CAPB.capb_carp_cd_ramo) = '50') then 'VAH'
            else NULL
        end) cod_seg_tp_seguro,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from    bi_corp_staging.rio6_cart_productos capu
   INNER JOIN bi_corp_staging.rio6_cart_prodplanes capb
        ON (capb.capb_carp_cd_ramo     = capu.capu_carp_cd_ramo and
            capb.capb_capu_cd_producto = capu.capu_cd_producto and
            capb.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
            capu.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
            );

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_DIM_SEG_TIPO_SEGURO as
SELECT  TPP.cod_seg_ramo,
        TRIM(CG.rv_low_value) ds_seg_ramo,
        TPP.cod_seg_producto,
        TPP.ds_seg_producto,
        TPP.cod_seg_plan,
        TPP.ds_seg_plan,
        TPP.cod_seg_tp_seguro,
        CASE TPP.cod_seg_tp_seguro
            when 'APE' THEN 'ACCIDENTES PERSONALES'
            when 'INC' THEN 'INCENDIO'
            when 'RCB' then 'FRAUDE BONIFICADO'
            when 'RCJ' then 'FRAUDE'
            when 'PCA' then 'PROTECCION CARTERA'
            when 'PMO' then 'PROTECCION MOVIL'
            when 'PIN' then 'PROTECCION INTELIGENTE'
            when 'PMO' then 'PROTECCION MOVIL'
            when 'SAL' then 'SALUD'
            when 'VID' then 'VIDA'
            when 'APE' then 'ACCIDENTES PERSONALES'
            when 'HOG' then 'HOGAR'
            when 'AUT' then 'AUTOS'
            when 'IPR' then 'GASTOS PROTEGIDOS'
            when 'PPA' then 'PROTECCION PAGOS'
            when 'COM' then 'INTEGRAL DE COMERCIO'
            when 'CON' then 'INTEGRAL DE CONSORCIO'
            when 'ART' then 'ART'
            when 'OTR' then 'OTROS'
            when 'PFE' then 'PROTECCION FEMENINA'
            when 'PMA' then 'PROTECCION MASCULINA'
            when 'GPR' then 'COMPRA PROTEGIDA'
            when 'VAH' then 'VIDA'
        end ds_seg_tp_seguro,
        TPP.partition_date
FROM TEMP_PRODPLAN TPP
LEFT JOIN bi_corp_staging.rio6_CG_REF_CODES CG
    ON (TPP.cod_seg_ramo = CAST(CG.rv_high_value AS BIGINT) and
        TPP.cod_seg_tp_seguro = CG.rv_type and
        CG.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
        CG.rv_domain = 'TABLEAU.RAMOS_PRODUCTOS'
       );

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SEG_AUT_MOT_INI as
select cast(cace.cace_carp_cd_ramo as bigint) cod_seg_ramo,
       case when trim(aumo.aumo_auth_cd_tipo_veh) in ('MOT','ATV','CUA') then 'MOTOS'
            else 'AUTOS' end ds_seg_ramo,
       trim(cace.cace_capu_cd_producto) cod_seg_producto,
       trim(cace.cace_capb_cd_plan) cod_seg_plan ,
       CASE when trim(aumo.aumo_auth_cd_tipo_veh)  in ('MOT','ATV','CUA') then 'MOT'
         ELSE 'AUT'
       END cod_seg_tp_seguro,
       CASE when trim(aumo.aumo_auth_cd_tipo_veh)  in ('MOT','ATV','CUA') then 'MOTOS'
         ELSE 'AUTOS'
       END ds_seg_tp_seguro,
       count(1) cantidad
from bi_corp_staging.rio6_cart_certificados cace,
       bi_corp_staging.rio6_autt_certificados auce,
       bi_corp_staging.rio6_autt_vehiculos auve,
       bi_corp_staging.rio6_autt_modelos aumo
 where auce.auce_cace_casu_cd_sucursal = cace.cace_casu_cd_sucursal
   and auce.auce_cace_carp_cd_ramo = cace.cace_carp_cd_ramo
   and auce.auce_cace_capo_nu_poliza = cace.cace_capo_nu_poliza
   and auce.auce_cace_nu_certificado = cace.cace_nu_certificado
   and auce.auce_auve_nu_serial_carroceria = auve.auve_nu_serial_carroceria
   and auve.auve_auma_cd_marca    = aumo.aumo_auma_cd_marca
   and auve.auve_aumo_cd_modelo   = aumo.aumo_cd_modelo
   AND cast(cace.cace_carp_cd_ramo as bigint) between 30 and 40
   and cace.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
   and auce.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
   and auve.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
   and aumo.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
   group by cast(cace.cace_carp_cd_ramo as bigint),
       trim(cace.cace_capu_cd_producto),
       trim(cace.cace_capb_cd_plan),
       case when trim(aumo.aumo_auth_cd_tipo_veh) in ('MOT','ATV','CUA') then 'MOTOS'
            else 'AUTOS' end,
       trim(cace.cace_capu_cd_producto),
       trim(cace.cace_capb_cd_plan),
       CASE when trim(aumo.aumo_auth_cd_tipo_veh)  in ('MOT','ATV','CUA') then 'MOT'
         ELSE 'AUT'
       END,
       CASE when trim(aumo.aumo_auth_cd_tipo_veh)  in ('MOT','ATV','CUA') then 'MOTOS'
         ELSE 'AUTOS'
       END;

-- Elimino los duplicados de seguros de AUTOS y MOTOS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_SEG_AUT_MOT as
select T.cod_seg_ramo,
       T.ds_seg_ramo,
       T.cod_seg_producto,
       T.cod_seg_plan,
       T.cod_seg_tp_seguro,
       T.ds_seg_tp_seguro
from (
    select I.cod_seg_ramo,
           I.ds_seg_ramo,
           I.cod_seg_producto,
           I.cod_seg_plan,
           I.cod_seg_tp_seguro,
           I.ds_seg_tp_seguro,
           row_number() over (partition by I.cod_seg_ramo,I.cod_seg_producto,I.cod_seg_plan order by I.cantidad desc ) as row_num
    from TEMP_SEG_AUT_MOT_INI I) T
WHERE T.row_num = 1;

INSERT OVERWRITE TABLE bi_corp_common.dim_seg_tipo_seguro
partition(partition_date)
SELECT T1.COD_SEG_RAMO,
       CASE WHEN COALESCE(T1.DS_SEG_RAMO,T2.DS_SEG_RAMO) IS NOT NULL THEN COALESCE(T1.DS_SEG_RAMO,T2.DS_SEG_RAMO)
            ELSE (CASE WHEN T1.COD_SEG_RAMO BETWEEN 30 AND 40 THEN 'AUTOS' ELSE 'OTROS' END)
       END ds_seg_ramo,
       T1.COD_SEG_PRODUCTO,
       T1.DS_SEG_PRODUCTO,
       T1.COD_SEG_PLAN,
       T1.DS_SEG_PLAN,
       CASE WHEN COALESCE(T1.COD_SEG_TP_SEGURO,T2.cod_seg_tp_seguro) IS NOT NULL THEN COALESCE(T1.COD_SEG_TP_SEGURO,T2.cod_seg_tp_seguro)
            ELSE (CASE WHEN T1.COD_SEG_RAMO BETWEEN 30 AND 40 THEN 'AUT' ELSE 'OTR' END)
       END cod_seg_tp_seguro,
       CASE WHEN COALESCE(T1.DS_SEG_TP_SEGURO,T2.DS_seg_tp_seguro) IS NOT NULL THEN COALESCE(T1.DS_SEG_TP_SEGURO,T2.DS_seg_tp_seguro)
            ELSE (CASE WHEN T1.COD_SEG_RAMO BETWEEN 30 AND 40 THEN 'AUTOS' ELSE 'OTROS' END)
       END ds_seg_tp_seguro,
       t1.partition_date
FROM TEMP_DIM_SEG_TIPO_SEGURO T1
LEFT JOIN TEMP_SEG_AUT_MOT T2
ON (T1.COD_SEG_RAMO = T2.COD_SEG_RAMO AND
     T1.COD_SEG_PRODUCTO = T2.COD_SEG_PRODUCTO AND
     T1.COD_SEG_PLAN = T2.COD_SEG_PLAN);