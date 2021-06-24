set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--armo tabla temporal TEMP_TP_CUENTAS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_TP_CUENTAS as
select distinct
      ctcu_tp_cuenta,
      ctcu_nm_cuenta,
      ctcu_in_cuenta
from   bi_corp_staging.rio6_cart_tipos_cuentas
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}';

--armo tabla temporal TEMP_RAMOS_PRODUCTOS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_RAMOS_PRODUCTOS as
SELECT  RV_LOW_VALUE,
        RV_HIGH_VALUE,
        RV_TYPE
FROM bi_corp_staging.rio6_CG_REF_CODES
WHERE RV_DOMAIN = 'TABLEAU.RAMOS_PRODUCTOS'
and partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-10-20','2020-10-20','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}');

--armo tabla temporal TEMP_TIPO_MERCADO
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_TIPO_MERCADO as
SELECT  RV_LOW_VALUE,
        RV_HIGH_VALUE,
        RV_TYPE
FROM bi_corp_staging.rio6_CG_REF_CODES
WHERE RV_DOMAIN = 'TABLEAU.TIPO_MERCADO'
and partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-18','2020-11-18','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}');

--------------Calculo la maxima particion PEDT001 ya que la misma se carga dias habiles
CREATE TEMPORARY TABLE tmp_maxpart001 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt001
 WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}';

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT001 as
select cast(p1.penumper as bigint) penumper,
     case when trim(p1.pepriape)=''
      then trim(p1.penomfan)
        else
          trim(concat(trim(p1.pepriape),',',trim(p1.penomper)))
     end penomper,
     trim(p1.petipdoc) petipdoc,
     (CASE trim(p1.petipdoc)
        WHEN 'N' THEN 'D'
          WHEN 'C' THEN 'L'
          WHEN 'I' THEN 'C'
          WHEN 'T' THEN 'J'
          WHEN 'L' THEN 'U'
          WHEN 'D' THEN 'I'
          WHEN 'X' THEN 'Q'
          ELSE trim(p1.petipdoc)
       END) petipdoc2,
     CAST(p1.penumdoc as BIGINT) penumdoc,
     CASE WHEN TRIM(p1.pesexper) = 'M' THEN 'F' ELSE TRIM(p1.pesexper) END pesexper,
     p1.pefecnac
from bi_corp_staging.malpe_pedt001 p1
inner join tmp_maxpart001 mx1 on p1.partition_date=mx1.partition_date;

--------------Calculo la maxima particion PEDT016 ya que la misma se carga dias habiles
CREATE TEMPORARY TABLE tmp_maxpart016 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt016
 WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}';

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT016 as
select cast(p16.penumper as bigint) penumper,
       cast(p16.penclref as bigint) penumper_viejo
from bi_corp_staging.malpe_pedt016 p16
inner join tmp_maxpart016 mx16 on p16.partition_date=mx16.partition_date
where p16.peestref = 'R';


-- Inserto en la tabla TMP_STK_SEG_SEGUROS
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_STK_SEG_SEGUROS as
select CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
       CAST(CACE.cace_capo_nu_poliza AS BIGINT) cod_seg_poliza,
       CAST(CACE.cace_nu_certificado AS BIGINT) cod_seg_certificado,
       CAST(SUBSTRING(TRIM(CACE.cace_nu_poliza_original),1,20) AS BIGINT) cod_seg_poliza_original,
       CAST(SUBSTRING(TRIM(CACE.cace_nu_poliza_original),21) AS BIGINT) cod_seg_certificado_original,
       TS.cod_seg_tipo_seguro,
       coalesce(TS.ds_seg_tipo_seguro,
                (CASE WHEN TRIM(TRP.rv_low_value) = 'VIDA CON AHORRO' then 'VIDA' else TRIM(TRP.rv_low_value) end),
                (CASE WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40 then 'AUTOS'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and TRIM(CACE.cace_capu_cd_producto) like '%PF%' then 'PROTECCION FEMENINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and SUBSTRING(TRIM(CACE.cace_capu_cd_producto),1,2) = 'PM' then 'PROTECCION MASCULINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) = 68 then 'OTROS' end)
               ) ds_seg_tipo_seguro,
       CAST(CACE.cace_carp_cd_ramo AS BIGINT) cod_seg_ramo,
       TS.ds_seg_ramo ds_seg_ramo,
       TRIM(CACE.cace_capu_cd_producto) cod_seg_producto,
       TS.ds_seg_producto ds_seg_producto,
       TRIM(CACE.cace_capb_cd_plan) cod_seg_plan,
       TS.ds_seg_plan ds_seg_plan,
       TRIM(CAPU.capu_cacy_cd_clase) cod_seg_clase,
       TRIM(TTM.rv_high_value) ds_seg_clase,
       case
            when CAST(CAPJ.capj_canl_cd_canal AS BIGINT) = 3 then CAST(CAZS.cazs_cd_zona AS BIGINT)
            else null
       end cod_seg_zona,
       CAST(CAPJ.capj_cd_sucursal AS BIGINT) cod_seg_sucursal,
       case
            when (CAPJ.capj_cd_sucursal is not null) then TRIM(CAPJ.capj_de_sucursal)
            else 'SUCURSAL INEXISTENTE'
       end ds_seg_sucursal,
       CAST(CANL.CANL_CD_CANAL AS BIGINT) cod_seg_canal,
       TRIM(canl.canl_de_canal) ds_seg_canal,
       TRIM(INAG.inag_de_agrupador) ds_seg_agrupador_canales,
       TRIM(TTM.rv_type) cod_seg_tipo_mercado,
       case
            when TRIM(TTM.rv_type) = 'OM' then 'OPEN MARKET'
            when TRIM(TTM.rv_type) = 'CR' then 'CREDIT RELATED'
            else null
       end ds_seg_tipo_mercado,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_completada) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_completada) end),'yyyy-MM-dd') dt_seg_fecha_alta_poliza_original,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_suscripcion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_suscripcion) end),'yyyy-MM-dd') dt_seg_fecha_suscripcion,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_desde) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_desde) end),'yyyy-MM-dd') dt_seg_fecha_desde_vigencia,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_hasta) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_hasta) end),'yyyy-MM-dd') dt_seg_fecha_hasta_vigencia,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_proxima_facturacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_proxima_facturacion) end),'yyyy-MM-dd') dt_seg_fecha_proxima_facturacion,
       TRIM(CACE.cace_cacn_cd_nacionalidad) ds_seg_tipo_doc,
       CAST(CACE.cace_cacn_nu_cedula_rif AS BIGINT) ds_seg_num_doc,
       CASE WHEN TPDT01.penomper IS NULL THEN TRIM(CACN.cacn_nm_apellido_razon) ELSE TPDT01.penomper END ds_per_nombre,
              CASE WHEN TRIM(CACE.cace_mt_saldo_deudor) is null or TRIM(CACE.cace_mt_saldo_deudor) = 'null' THEN CAST(CACE.cace_mt_suma_asegurada AS bigint) ELSE CAST(CACE.cace_mt_saldo_deudor AS bigint) END fc_seg_suma_asegurada,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))
       end AS DECIMAL (15,2)) fc_seg_premio,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))
       end AS DECIMAL(15,2)) fc_seg_prima,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))
       end AS DECIMAL(15,2)) fc_seg_comision,
       TRIM(CACE.cace_cacn_tdoc_tomador) ds_seg_tipo_doc_tomador,
       TRIM(CACE.cace_cacn_ndoc_tomador) ds_seg_num_doc_tomador,
       CASE WHEN TRIM(hoce.hoce_nm_beneficiario) = 'null' then null else TRIM(hoce.hoce_nm_beneficiario) end ds_seg_nombre_beneficiario,
       TRIM(CAFP.cafp_cd_fr_pago) cod_seg_forma_pago,
       TRIM(CAFP.cafp_de_fr_pago) ds_seg_forma_pago,
       TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
       TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
       CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
       CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
       TRIM(CAES.caes_de_estado) ds_seg_provincia,
       CASE WHEN trim(CACE.cace_zn_postal) = 'null' then null else trim(CACE.cace_zn_postal) end ds_seg_cod_postal,
       TRIM(caci.caci_de_ciudad) ds_seg_ciudad,
       CASE WHEN TRIM(hoce.hoce_ub_riesgo1) = 'null' and TRIM(hoce.hoce_de_auxiliar) = 'null' then null
            WHEN TRIM(hoce.hoce_ub_riesgo1) <> 'null' and TRIM(hoce.hoce_de_auxiliar) = 'null' then TRIM(hoce.hoce_ub_riesgo1)
            WHEN TRIM(hoce.hoce_ub_riesgo1) = 'null' and TRIM(hoce.hoce_de_auxiliar) <> 'null' then TRIM(hoce.hoce_de_auxiliar)
            else CONCAT(TRIM(hoce.hoce_ub_riesgo1),' ',TRIM(hoce.hoce_de_auxiliar)) end
        ds_seg_domicilio,
       cast(NULL as string) cod_seg_tipo_vehiculo,
       cast(NULL as string) ds_seg_tipo_vehiculo,
       cast(NULL as string) cod_seg_serial_carroceria,
       cast(NULL as string) cod_seg_serial_motor,
       cast(NULL as string) cod_seg_marca,
       cast(NULL as string) ds_seg_marca,
       cast(NULL as string) cod_seg_modelo,
       cast(NULL as string) ds_seg_modelo,
       cast(NULL as bigint) int_seg_ano_fabricacion,
       cast(NULL as string) cod_seg_placa,
       hotv.hotv_tp_vivienda cod_seg_tipo_vivienda,
       hotv.hotv_de_vivienda ds_seg_tipo_vivienda,
       hoce.hoce_an_construccion cod_seg_sup_cubierta,
       TRIM(CACM.cacm_de_compania) ds_seg_compania,
       case when nvl(CACE.cace_poliza_digital,'N') = 'S' then 1 else 0 end flag_seg_poliza_digital,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from bi_corp_staging.rio6_cart_certificados CACE
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CACE.cace_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPU.capu_cd_producto and
            CACE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
            CAPU.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
           )
  LEFT JOIN bi_corp_staging.rio6_hogt_certificados HOCE
    ON (CACE.cace_casu_cd_sucursal = HOCE.hoce_cace_casu_cd_sucursal and
      CACE.cace_carp_cd_ramo     = HOCE.hoce_cace_carp_cd_ramo and
      CACE.cace_capo_nu_poliza   = HOCE.hoce_cace_capo_nu_poliza and
      CACE.cace_nu_certificado   = HOCE.hoce_cace_nu_certificado and
      HOCE.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-29','2020-11-29','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
       )
  LEFT JOIN bi_corp_staging.rio6_hogt_tp_vivienda HOTV
    ON (HOCE.hoce_tp_vivienda = HOTV.hotv_tp_vivienda and
      HOTV.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-29','2020-11-29','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
       )
  LEFT JOIN bi_corp_staging.rio6_cart_ciudades CACI
    ON (CACE.cace_caes_cd_estado = CACI.caci_caes_cd_estado and
        CACE.cace_caci_cd_ciudad = CACI.caci_cd_ciudad and
      CACI.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
       )
  LEFT JOIN bi_corp_staging.rio6_cart_estados CAES
    ON (CACE.cace_caes_cd_estado = CAES.caes_cd_estado and
        CAES.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAPU.capu_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAPU.capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_clientes CACN
      ON (CACE.cace_cacn_cd_nacionalidad = CACN.cacn_cd_nacionalidad and
          CACE.cace_cacn_nu_cedula_rif   = CACN.cacn_nu_cedula_rif and
          CACN.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_PEDT001 TPDT01
        ON (CACE.cace_cacn_cd_nacionalidad = TPDT01.petipdoc2 and
            CAST(CACE.cace_cacn_nu_cedula_rif AS BIGINT) = TPDT01.penumdoc and
            TRIM(CACN.cacn_cd_sexo) = TPDT01.pesexper and
            to_date(CACN.cacn_fe_nacimiento) = TPDT01.pefecnac)
    LEFT JOIN bi_corp_staging.rio6_cart_domicilios_bancarios CADM
        ON (CACE.cace_cacn_cd_nacionalidad = CADM.cadm_cacn_cd_nacionalidad and
            CACE.cace_cacn_nu_cedula_rif   = CADM.cadm_cacn_nu_cedula_rif and
            CACE.cace_cadm_nu_domicilio_db = CADM.cadm_nu_domicilio and
            CADM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_TP_CUENTAS TTC
            ON (CADM.cadm_tp_cuenta = TTC.ctcu_tp_cuenta)
    LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco CAPJ
        ON (CACE.cace_cazb_cd_sucursal = CAPJ.capj_cd_sucursal and
            CAPJ.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_zona_sucursal CAZS
        ON (CACE.cace_cazb_cd_sucursal = CAZS.cazs_cd_sucursal and
            CAZS.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_canales  CANL
        ON (CAPJ.capj_canl_cd_canal = CANL.canl_cd_canal and
            CANL.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores_canales INGC
        ON (CANL.canl_cd_canal = INGC.ingc_inca_cd_canal and
            INGC.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores INAG
        ON (INGC.ingc_inag_cd_agrupador = INAG.inag_cd_agrupador and
            INAG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (CAST(CACE.cace_carp_cd_ramo AS BIGINT) = TS.cod_seg_ramo and
            TRIM(CACE.cace_capu_cd_producto) = TS.cod_seg_producto and
            TRIM(CACE.cace_capb_cd_plan) = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_RAMOS_PRODUCTOS TRP
        ON (TRP.RV_HIGH_VALUE = CAPB.capb_carp_cd_ramo AND
            TRIM(TRP.rv_type)       = TS.cod_seg_tipo_seguro)
    LEFT JOIN TEMP_TIPO_MERCADO TTM
        ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
WHERE CAST(CACE.cace_st_certificado AS BIGINT)  in (1,30)
    AND (
                (TRIM(CAPU.capu_in_colectivo) != 'S')
            OR
                 (
                    (TRIM(CAPU.capu_in_colectivo) = 'S') and (CAST(CACE.cace_nu_certificado AS BIGINT) != 0)
                 )
        )
    AND NOT (
                ((CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18) AND (TRIM(CAPU.capu_cacy_cd_clase) IN ('IP','PP')) AND (CAPB.capb_cd_clasificacion = 'REL'))
            OR
                ((CAST(CACE.cace_carp_cd_ramo AS BIGINT) IN (60,62,64,66,68,70,72,74)) AND (CAST(CACE.cace_nu_certificado AS BIGINT) > 1000))
            OR
                (CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40)
            )
UNION ALL
select CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
       CAST(CACE.cace_capo_nu_poliza AS BIGINT) cod_seg_poliza,
       CAST(CACE.cace_nu_certificado AS BIGINT) cod_seg_certificado,
       CAST(SUBSTRING(TRIM(CACE.cace_nu_poliza_original),1,20) AS BIGINT) cod_seg_poliza_original,
       CAST(SUBSTRING(TRIM(CACE.cace_nu_poliza_original),21) AS BIGINT) cod_seg_certificado_original,
       TS.cod_seg_tipo_seguro,
       coalesce(TS.ds_seg_tipo_seguro,'AUTOS') ds_seg_tipo_seguro,
       CAST(CACE.cace_carp_cd_ramo AS BIGINT) cod_seg_ramo,
       TS.ds_seg_ramo,
       TRIM(CACE.cace_capu_cd_producto) cod_seg_producto,
       TS.ds_seg_producto,
       TRIM(CACE.cace_capb_cd_plan) cod_seg_plan,
       TS.ds_seg_plan ds_seg_plan,
       TRIM(CAPU.capu_cacy_cd_clase) cod_seg_clase,
       TRIM(TTM.rv_high_value) ds_seg_clase,
       case
            when CAST(CAPJ.capj_canl_cd_canal AS BIGINT) = 3 then CAST(CAZS.cazs_cd_zona AS BIGINT)
            else null
       end cod_seg_zona,
       CAST(CAPJ.capj_cd_sucursal AS BIGINT) cod_seg_sucursal,
       case
            when (CAPJ.capj_cd_sucursal is not null) then TRIM(CAPJ.capj_de_sucursal)
            else 'SUCURSAL INEXISTENTE'
       end ds_seg_sucursal,
       CAST(CANL.CANL_CD_CANAL AS BIGINT) cod_seg_canal,
       TRIM(canl.canl_de_canal) ds_seg_canal,
       TRIM(INAG.inag_de_agrupador) ds_seg_agrupador_canales,
       TRIM(TTM.rv_type) cod_seg_tipo_mercado,
       case
            when TRIM(TTM.rv_type) = 'OM' then 'OPEN MARKET'
            when TRIM(TTM.rv_type) = 'CR' then 'CREDIT RELATED'
            else null
       end ds_seg_tipo_mercado,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_completada) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_completada) end),'yyyy-MM-dd') dt_seg_fecha_alta_poliza_original,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_suscripcion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_suscripcion) end),'yyyy-MM-dd') dt_seg_fecha_suscripcion,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_desde) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_desde) end),'yyyy-MM-dd') dt_seg_fecha_desde_vigencia,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_hasta) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_hasta) end),'yyyy-MM-dd') dt_seg_fecha_hasta_vigencia,
       from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_proxima_facturacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_proxima_facturacion) end),'yyyy-MM-dd') dt_seg_fecha_proxima_facturacion,
       TRIM(CACE.cace_cacn_cd_nacionalidad) ds_seg_tipo_doc,
       CAST(CACE.cace_cacn_nu_cedula_rif AS BIGINT) ds_seg_num_doc,
       CASE WHEN TPDT01.penomper IS NULL THEN TRIM(CACN.cacn_nm_apellido_razon) ELSE TPDT01.penomper END ds_per_nombre,
       CASE WHEN TRIM(CACE.cace_mt_saldo_deudor) is null or TRIM(CACE.cace_mt_saldo_deudor) = 'null' THEN CAST(CACE.cace_mt_suma_asegurada AS bigint) ELSE CAST(CACE.cace_mt_saldo_deudor AS bigint) END fc_seg_suma_asegurada,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))
       end AS DECIMAL (15,2)) fc_seg_premio,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))
       end AS DECIMAL(15,2)) fc_seg_prima,
       CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))
       end AS DECIMAL(15,2)) fc_seg_comision,
              TRIM(CACE.cace_cacn_tdoc_tomador) ds_seg_tipo_doc_tomador,
       TRIM(CACE.cace_cacn_ndoc_tomador) ds_seg_num_doc_tomador,
       cast(NULL as string) ds_seg_nombre_beneficiario,
       TRIM(CAFP.cafp_cd_fr_pago) cod_seg_forma_pago,
       TRIM(CAFP.cafp_de_fr_pago) ds_seg_forma_pago,
       TRIM(ttc.ctcu_in_cuenta) cod_seg_tIpO_cuenta,
       TRIM(ttc.ctcu_nm_cuenta) ds_seg_tIpO_cuenta,
       CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
       CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
       TRIM(CAES.caes_de_estado) ds_seg_provincia,
       CASE WHEN trim(CACE.cace_zn_postal) = 'null' then null else trim(CACE.cace_zn_postal) end ds_seg_cod_postal,
       TRIM(caci.caci_de_ciudad) ds_seg_ciudad,
       TRIM(AUCE.auce_direccion) ds_seg_domicilio,
       AUVE.auve_auth_cd_tipo_vehiculo cod_seg_tipo_vehiculo,
       auth.auth_de_tipo_vehiculo ds_seg_tipo_vehiculo,
       auve.auve_nu_serial_carroceria cod_seg_serial_carroceria,
       auve.auve_nu_serial_motor cod_seg_serial_motor,
       auve.auve_auma_cd_marca cod_seg_marca,
       AUMA.auma_de_marca ds_seg_marca,
       auve.auve_aumo_cd_modelo cod_seg_modelo,
       AUMO.aumo_de_modelo ds_seg_modelo,
       auve.auve_an_fabricacion int_seg_ano_fabricacion,
       auve.auve_nu_placa cod_seg_placa,
       cast(NULL as string) cod_seg_tipo_vivienda,
       cast(NULL as string) ds_seg_tipo_vivienda,
       cast(NULL as string) cod_seg_sup_cubierta,
       TRIM(CACM.cacm_de_compania) ds_seg_compania,
       case when nvl(CACE.cace_poliza_digital,'N') = 'S' then 1 else 0 end flag_seg_poliza_digital,
      '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from bi_corp_staging.rio6_cart_certificados CACE
    INNER JOIN bi_corp_staging.rio6_autt_certificados AUCE
        ON (CACE.cace_casu_cd_sucursal = AUCE.auce_cace_casu_cd_sucursal and
            CACE.cace_carp_cd_ramo     = AUCE.auce_cace_carp_cd_ramo and
            CACE.cace_capo_nu_poliza   = AUCE.auce_cace_capo_nu_poliza and
            CACE.cace_nu_certificado   = AUCE.auce_cace_nu_certificado and
            CACE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
            AUCE.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-24','2020-11-24','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'))
  LEFT JOIN bi_corp_staging.rio6_cart_estados CAES
    ON (CACE.cace_caes_cd_estado = CAES.caes_cd_estado and
        CAES.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
  LEFT JOIN bi_corp_staging.rio6_cart_ciudades CACI
    ON (CACE.cace_caes_cd_estado = CACI.caci_caes_cd_estado and
        CACE.cace_caci_cd_ciudad = CACI.caci_cd_ciudad and
      CACI.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
       )
    INNER JOIN bi_corp_staging.rio6_autt_vehiculos AUVE
        ON (AUVE.auve_nu_serial_carroceria = AUCE.auce_auve_nu_serial_carroceria and
            AUVE.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-24','2020-11-24','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'))
    INNER JOIN bi_corp_staging.rio6_autt_tipos_vehiculos AUTH
      ON (AUTH.AUTH_CD_TIPO_VEHICULO = auve.auve_auth_cd_tipo_vehiculo and
          auth.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    INNER JOIN bi_corp_staging.rio6_autt_marcas AUMA
        ON (AUMA.auma_cd_marca = AUVE.auve_auma_cd_marca and
            AUMA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    INNER JOIN bi_corp_staging.rio6_autt_modelos AUMO
        ON (AUVE.auve_auma_cd_marca = AUMO.aumo_auma_cd_marca and
            AUVE.auve_aumo_cd_modelo = AUMO.aumo_cd_modelo and
            AUMO.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CACE.cace_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPU.capu_cd_producto and
            CAPU.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAPU.capu_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAPU.capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_clientes CACN
      ON (CACE.cace_cacn_cd_nacionalidad = CACN.cacn_cd_nacionalidad and
            CACE.cace_cacn_nu_cedula_rif   = CACN.cacn_nu_cedula_rif and
            CACN.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_PEDT001 TPDT01
        ON (CACE.cace_cacn_cd_nacionalidad = TPDT01.petipdoc2 and
            CAST(CACE.cace_cacn_nu_cedula_rif AS BIGINT) = TPDT01.penumdoc and
            TRIM(CACN.cacn_cd_sexo) = TPDT01.pesexper and
            to_date(CACN.cacn_fe_nacimiento) = TPDT01.pefecnac)
    LEFT JOIN bi_corp_staging.rio6_cart_domicilios_bancarios CADM
        ON (CACE.cace_cacn_cd_nacionalidad = CADM.cadm_cacn_cd_nacionalidad and
            CACE.cace_cacn_nu_cedula_rif   = CADM.cadm_cacn_nu_cedula_rif and
            CACE.cace_cadm_nu_domicilio_db = CADM.cadm_nu_domicilio and
            CADM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_TP_CUENTAS TTC
        ON (CADM.cadm_tp_cuenta = TTC.ctcu_tp_cuenta)
    LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco CAPJ
        ON (CACE.cace_cazb_cd_sucursal = CAPJ.capj_cd_sucursal and
            CAPJ.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_zona_sucursal CAZS
        ON (CACE.cace_cazb_cd_sucursal = CAZS.cazs_cd_sucursal and
            CAZS.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_canales  CANL
        ON (CAPJ.capj_canl_cd_canal = CANL.canl_cd_canal and
            CANL.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores_canales INGC
        ON (CANL.canl_cd_canal = INGC.ingc_inca_cd_canal and
            INGC.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores INAG
        ON (INGC.ingc_inag_cd_agrupador = INAG.inag_cd_agrupador and
            INAG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (CAST(CACE.cace_carp_cd_ramo AS BIGINT) = TS.cod_seg_ramo and
            TRIM(CACE.cace_capu_cd_producto) = TS.cod_seg_producto and
            TRIM(CACE.cace_capb_cd_plan) = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_TIPO_MERCADO TTM
        ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
  where CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40
    AND CAST(CACE.cace_st_certificado AS BIGINT) in (1,30);

-- Inserto en la tabla stk_seg_seguros
INSERT OVERWRITE TABLE bi_corp_common.stk_seg_seguros
partition(partition_date)
select COALESCE(p.penumper,t.cod_per_nup)  cod_per_nup,
       t.cod_seg_poliza,
       t.cod_seg_certificado,
       t.cod_seg_poliza_original,
       t.cod_seg_certificado_original,
       t.cod_seg_tipo_seguro,
       t.ds_seg_tipo_seguro,
       t.cod_seg_ramo,
       t.ds_seg_ramo,
       t.cod_seg_producto,
       t.ds_seg_producto,
       t.cod_seg_plan,
       t.ds_seg_plan,
       t.cod_seg_clase,
       t.ds_seg_clase,
       t.cod_seg_zona,
       t.cod_seg_sucursal,
       t.ds_seg_sucursal,
       t.cod_seg_canal,
       t.ds_seg_canal,
       t.ds_seg_agrupador_canales,
       t.cod_seg_tipo_mercado,
       t.ds_seg_tipo_mercado,
       t.dt_seg_fecha_alta_poliza_original,
       t.dt_seg_fecha_suscripcion,
       t.dt_seg_fecha_desde_vigencia,
       t.dt_seg_fecha_hasta_vigencia,
       t.dt_seg_fecha_proxima_facturacion,
       t.ds_seg_tipo_doc,
       t.ds_seg_num_doc,
       t.ds_per_nombre,
       t.fc_seg_suma_asegurada,
       t.fc_seg_premio,
       t.fc_seg_prima,
       t.fc_seg_comision,
       t.ds_seg_tipo_doc_tomador,
       t.ds_seg_num_doc_tomador,
       t.ds_seg_nombre_beneficiario,
       t.cod_seg_forma_pago,
       t.ds_seg_forma_pago,
       t.cod_seg_tipo_cuenta,
       t.ds_seg_tipo_cuenta,
       t.cod_seg_sucursal_cuenta,
       t.cod_seg_cuenta,
       t.ds_seg_provincia,
       t.ds_seg_cod_postal,
       t.ds_seg_ciudad,
       t.ds_seg_domicilio,
       t.cod_seg_tipo_vehiculo,
       t.ds_seg_tipo_vehiculo,
       t.cod_seg_serial_carroceria,
       t.cod_seg_serial_motor,
       t.cod_seg_marca,
       t.ds_seg_marca,
       t.cod_seg_modelo,
       t.ds_seg_modelo,
       t.int_seg_ano_fabricacion,
       t.cod_seg_placa,
       t.cod_seg_tipo_vivienda,
       t.ds_seg_tipo_vivienda,
       t.cod_seg_sup_cubierta,
       t.ds_seg_compania,
       t.flag_seg_poliza_digital,
       t.partition_date
from TMP_STK_SEG_SEGUROS t
left join  TEMP_PEDT016 p
on (t.cod_per_nup = p.penumper_viejo);