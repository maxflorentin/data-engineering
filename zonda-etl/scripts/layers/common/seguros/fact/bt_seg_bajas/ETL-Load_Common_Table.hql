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

-- Inserto en la tabla TMP_SEG_BAJAS
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_SEG_BAJAS as
select   CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
         CAST(CACE.cace_capo_nu_poliza AS BIGINT) cod_seg_poliza,
         CAST(CACE.cace_nu_certificado AS BIGINT) cod_seg_certificado,
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
         from_unixtime(UNIX_TIMESTAMP(case when trim(CAOD.caod_fe_operacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CAOD.caod_fe_operacion) end),'yyyy-MM-dd') dt_seg_fecha_operacion,
         from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_suscripcion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_suscripcion) end),'yyyy-MM-dd') dt_seg_fecha_alta,
         from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_completada) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_completada) end),'yyyy-MM-dd') dt_seg_fecha_alta_poliza_original,
         CAST(CAOD.caod_caop_cd_operacion AS BIGINT) cod_seg_operacion,
         TRIM(CAOP.caop_de_operacion) ds_seg_operacion,
         TRIM(CAOD.caod_cd_usuario) cod_seg_usuario,
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
         TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
         TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
         CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
         CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
         TRIM(CACM.cacm_de_compania) ds_seg_compania,
         case
         when trim(CANA.cana_cd_familia) is not null then TRIM(CANA.cana_cd_familia)
         else 'XX'
        end cod_seg_familia,
        case trim(CANA.cana_cd_familia)
             when 'XX' then 'SIN DESCRIPCION'
             when 'FI' then 'BAJAS FINANCIERAS'
             when 'MV' then 'VENTA INCORRECTA'
             when 'DE' then 'BAJA POR DESISTIMIENTO'
             when 'FB' then 'FALSAS BAJAS'
             when 'CO' then 'BAJAS COMPAÑIA'
             else 'SIN DESCRIPCION'
        end ds_seg_familia,
        case
         when CAST(CANA.cana_cd_causa_anulacion as bigint) is not null then CAST(CANA.cana_cd_causa_anulacion as bigint)
         else 999
        end cod_seg_anulacion,
        case
         when CAST(CANA.cana_cd_causa_anulacion as bigint) is not null then TRIM(CANA.cana_de_causa_anulacion)
         else 'SIN DESCRIPCION'
        end ds_seg_anulacion,
        CAOD.caod_rowid cod_seg_rowid,
        to_date(CAOD.caod_fe_operacion) caod_fe_operacion,
        CAST(CACE.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
    INNER JOIN bi_corp_staging.rio6_cart_operaciones CAOP
        ON (CAOD.caod_caop_cd_operacion = CAOP.caop_cd_operacion and
            CAOD.caod_cd_sub_operacion  = CAOP.caop_cd_sub_operacion and
            CAOD.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
            CAOP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
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
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CACE.cace_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPU.capu_cd_producto and
            CAPU.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAPU.capu_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAPU.capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (CAST(CACE.cace_carp_cd_ramo AS BIGINT)     = TS.cod_seg_ramo and
            TRIM(CACE.cace_capu_cd_producto) = TS.cod_seg_producto and
            TRIM(CACE.cace_capb_cd_plan)     = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
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
    LEFT JOIN TEMP_RAMOS_PRODUCTOS TRP
        ON (TRP.RV_HIGH_VALUE = CAPB.capb_carp_cd_ramo AND
            TRIM(TRP.rv_type)       = TS.cod_seg_tipo_seguro)
    LEFT JOIN TEMP_TIPO_MERCADO TTM
        ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
    LEFT JOIN bi_corp_staging.rio6_cart_causas_anulacion_recibos CANA
        ON (
           CASE when cast(CAOD.caod_cd_accion_evento as bigint) is null then cast(CACE.cace_cd_causa_anulacion as bigint)
               else cast(CAOD.caod_cd_accion_evento as bigint)
           end = cast(CANA.cana_cd_causa_anulacion as bigint)
           and CANA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
           )
WHERE NOT (
            ((CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18) AND (CAPU.capu_cacy_cd_clase IN ('IP','PP')) AND (CAPB.capb_cd_clasificacion = 'REL'))
            OR
            ((CAST(CACE.cace_carp_cd_ramo AS BIGINT) IN (60,62,64,66,68,70,72,74)) AND (CAST(CACE.cace_nu_certificado AS BIGINT) > 1000))
            OR
            (CAST(CAOD.caod_carp_cd_ramo AS BIGINT) between 30 and 40)
          )
  AND CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1003
  AND CAST(CAOD.caod_cd_sub_operacion AS BIGINT) = 0
 UNION ALL
  select CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
        CAST(CACE.cace_capo_nu_poliza AS BIGINT) cod_seg_poliza,
        CAST(CACE.cace_nu_certificado AS BIGINT) cod_seg_certificado,
        TS.cod_seg_tipo_seguro,
        coalesce(TS.ds_seg_tipo_seguro,'AUTOS') ds_seg_tipo_seguro,
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
        from_unixtime(UNIX_TIMESTAMP(case when trim(CAOD.caod_fe_operacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CAOD.caod_fe_operacion) end),'yyyy-MM-dd') dt_seg_fecha_operacion,
        from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_suscripcion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_suscripcion) end),'yyyy-MM-dd') dt_seg_fecha_alta,
        from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_completada) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_completada) end),'yyyy-MM-dd') dt_seg_fecha_alta_poliza_original,
        CAST(CAOD.caod_caop_cd_operacion AS BIGINT) cod_seg_operacion,
        TRIM(CAOP.caop_de_operacion) ds_seg_operacion,
        TRIM(CAOD.caod_cd_usuario) cod_seg_usuario,
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
        TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
        TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
        TRIM(CACM.cacm_de_compania) ds_seg_compania,
        case
         when trim(CANA.cana_cd_familia) is not null then TRIM(CANA.cana_cd_familia)
         else 'XX'
        end cod_seg_familia,
        case trim(CANA.cana_cd_familia)
             when 'FI' then 'BAJAS FINANCIERAS'
             when 'MV' then 'VENTA INCORRECTA'
             when 'DE' then 'BAJA POR DESISTIMIENTO'
             when 'FB' then 'FALSAS BAJAS'
             when 'CO' then 'BAJAS COMPAÑIA'
             else 'SIN DESCRIPCION'
        end ds_seg_familia,
        case
         when CAST(CANA.cana_cd_causa_anulacion as bigint) is not null then CAST(CANA.cana_cd_causa_anulacion as bigint)
         else 999
        end cod_seg_anulacion,
        case
         when CAST(CANA.cana_cd_causa_anulacion as bigint) is not null then TRIM(CANA.cana_de_causa_anulacion)
         else 'SIN DESCRIPCION'
        end ds_seg_anulacion,
        CAOD.caod_rowid cod_seg_rowid,
        to_date(CAOD.caod_fe_operacion) caod_fe_operacion,
        CAST(CACE.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
    INNER JOIN bi_corp_staging.rio6_cart_operaciones CAOP
        ON (CAOD.caod_caop_cd_operacion = CAOP.caop_cd_operacion and
        CAOD.caod_cd_sub_operacion  = CAOP.caop_cd_sub_operacion and
        CAOD.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
        CAOP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    INNER JOIN bi_corp_staging.rio6_autt_certificados AUCE
        ON (CACE.cace_casu_cd_sucursal = AUCE.auce_cace_casu_cd_sucursal and
            CACE.cace_carp_cd_ramo     = AUCE.auce_cace_carp_cd_ramo and
            CACE.cace_capo_nu_poliza   = AUCE.auce_cace_capo_nu_poliza and
            CACE.cace_nu_certificado   = AUCE.auce_cace_nu_certificado and
            AUCE.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-24','2020-11-24','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'))
    INNER JOIN bi_corp_staging.rio6_autt_vehiculos AUVE
        ON (AUVE.auve_nu_serial_carroceria = AUCE.auce_auve_nu_serial_carroceria and
            AUVE.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-11-24','2020-11-24','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'))
    INNER JOIN bi_corp_staging.rio6_autt_modelos AUMO
        ON (AUVE.auve_auma_cd_marca = AUMO.aumo_auma_cd_marca and
            AUVE.auve_aumo_cd_modelo = AUMO.aumo_cd_modelo and
            AUMO.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
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
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CACE.cace_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPU.capu_cd_producto and
            CAPU.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAPU.capu_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAPU.capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco CAPJ
        ON (CACE.cace_cazb_cd_sucursal = CAPJ.capj_cd_sucursal and
            CAPJ.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (CAST(CACE.cace_carp_cd_ramo AS BIGINT)     = TS.cod_seg_ramo and
            TRIM(CACE.cace_capu_cd_producto) = TS.cod_seg_producto and
            TRIM(CACE.cace_capb_cd_plan)     = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
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
    LEFT JOIN TEMP_TIPO_MERCADO TTM
        ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
    LEFT JOIN bi_corp_staging.rio6_cart_causas_anulacion_recibos CANA
        ON (CASE when cast(CAOD.caod_cd_accion_evento as bigint) is null then cast(CACE.cace_cd_causa_anulacion as bigint)
               else cast(CAOD.caod_cd_accion_evento as bigint)
           end = cast(CANA.cana_cd_causa_anulacion as bigint)
           and CANA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
  WHERE (CAST(CAOD.caod_carp_cd_ramo AS BIGINT) between 30 and 40)
  AND CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1003
  AND CAST(CAOD.caod_cd_sub_operacion AS BIGINT) = 0;

 -- Inserto en la tabla TMP_EXISTS1
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_EXISTS1 as
select CAST(CACEB.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
       CAST(CACEB.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
       to_date(CAODB.caod_fe_operacion) caod_fe_operacion,
       CAODB.caod_rowid
    from bi_corp_staging.rio6_cart_certificados CACEB
   INNER JOIN bi_corp_staging.rio6_cart_operaciones_diarias CAODB
    ON (CAODB.caod_casu_cd_sucursal    = CACEB.cace_casu_cd_sucursal and
        CAODB.caod_carp_cd_ramo        = CACEB.cace_carp_cd_ramo and
        CAODB.caod_capo_nu_poliza      = CACEB.cace_capo_nu_poliza and
        CAODB.caod_cace_nu_certificado = CACEB.cace_nu_certificado and
        CACEB.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
        CAODB.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
       )
    where CAST(CAODB.caod_caop_cd_operacion AS BIGINT) = 1003
      and CAST(CAODB.caod_cd_sub_operacion AS BIGINT)   = 0;

-- Inserto en la tabla TMP_NOT_EXISTS1
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_NOT_EXISTS1 as
SELECT A.cace_carp_cd_ramo,A.cace_nu_poliza_original,A.caod_fe_operacion
FROM (
select CAST(CACEA.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
       CAST(CACEA.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
       to_date(CAODA.caod_fe_operacion) caod_fe_operacion ,
       CAODA.caod_rowid
from bi_corp_staging.rio6_cart_certificados CACEA
INNER JOIN bi_corp_staging.rio6_cart_operaciones_diarias CAODA
ON (CAODA.caod_casu_cd_sucursal    = CACEA.cace_casu_cd_sucursal and
    CAODA.caod_carp_cd_ramo        = CACEA.cace_carp_cd_ramo and
    CAODA.caod_capo_nu_poliza      = CACEA.cace_capo_nu_poliza and
    CAODA.caod_cace_nu_certificado = CACEA.cace_nu_certificado and
    CACEA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
    CAODA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
    )
where CAST(CAODA.caod_caop_cd_operacion AS BIGINT) = 1003
  and CAST(CAODA.caod_cd_sub_operacion AS BIGINT)   = 0
  and to_date(CAODA.caod_fe_operacion) not between to_date(CACEA.cace_fe_desde) and to_date(date_sub(CACEA.cace_fe_hasta,1))
) A
INNER JOIN TMP_EXISTS1 TMPE1
 ON (A.cace_carp_cd_ramo     = TMPE1.cace_carp_cd_ramo and
     A.cace_nu_poliza_original = TMPE1.cace_nu_poliza_original and
     to_date(A.caod_fe_operacion) = TMPE1.caod_fe_operacion
    )
 where A.caod_rowid  != TMPE1.caod_rowid;


 -- Inserto en la tabla TMP_EXISTS_ROWID
CREATE TEMPORARY TABLE IF NOT EXISTS TMP_EXISTS_ROWID as
select CAST(CACE3.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
CAST(CACE3.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
to_date(CAOD3.caod_fe_operacion) caod_fe_operacion,
       MAX(CAOD3.caod_rowid) max_caod_rowid
    from bi_corp_staging.rio6_cart_certificados CACE3
   INNER JOIN bi_corp_staging.rio6_cart_operaciones_diarias CAOD3
    ON (CAOD3.caod_casu_cd_sucursal    = CACE3.cace_casu_cd_sucursal and
        CAOD3.caod_carp_cd_ramo        = CACE3.cace_carp_cd_ramo and
        CAOD3.caod_capo_nu_poliza      = CACE3.cace_capo_nu_poliza and
        CAOD3.caod_cace_nu_certificado = CACE3.cace_nu_certificado and
        CACE3.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
        CAOD3.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
       )
    where CAST(CAOD3.caod_caop_cd_operacion AS BIGINT) = 1003
      and CAST(CAOD3.caod_cd_sub_operacion AS BIGINT)   = 0
      and to_date(CAOD3.caod_fe_operacion) between to_date(CACE3.cace_fe_desde) and date_sub(to_date(CACE3.cace_fe_hasta), 1)
      group by CAST(CACE3.cace_carp_cd_ramo AS BIGINT),CAST(CACE3.cace_nu_poliza_original AS BIGINT),CAOD3.caod_fe_operacion;

-- Inserto en la tabla bt_seg_bajas
INSERT OVERWRITE TABLE bi_corp_common.bt_seg_bajas
partition(partition_date)
SELECT DISTINCT
         A.cod_per_nup,
         A.cod_seg_poliza,
         A.cod_seg_certificado,
         A.cod_seg_tipo_seguro,
         A.ds_seg_tipo_seguro,
         A.cod_seg_ramo,
         A.ds_seg_ramo,
         A.cod_seg_producto,
         A.ds_seg_producto,
         A.cod_seg_plan,
         A.ds_seg_plan,
         A.cod_seg_clase,
         A.ds_seg_clase,
         A.cod_seg_zona,
         A.cod_seg_sucursal,
         A.ds_seg_sucursal,
         A.cod_seg_canal,
         A.ds_seg_canal,
         A.ds_seg_agrupador_canales,
         A.cod_seg_tipo_mercado,
         A.ds_seg_tipo_mercado,
         A.dt_seg_fecha_operacion,
         A.dt_seg_fecha_alta,
         A.dt_seg_fecha_alta_poliza_original,
         A.cod_seg_operacion,
         A.ds_seg_operacion,
         A.cod_seg_usuario,
         A.ds_seg_tipo_doc,
         A.ds_seg_num_doc,
         A.ds_per_nombre,
         A.fc_seg_suma_asegurada,
         A.fc_seg_premio,
         A.fc_seg_prima,
         A.fc_seg_comision,
         A.cod_seg_tipo_cuenta,
         A.ds_seg_tipo_cuenta,
         A.cod_seg_sucursal_cuenta,
         A.cod_seg_cuenta,
         A.ds_seg_compania,
         A.cod_seg_familia,
         A.ds_seg_familia,
         A.cod_seg_anulacion,
         A.ds_seg_anulacion,
         A.partition_date
from (
      select cod_per_nup,
             cod_seg_poliza,
             cod_seg_certificado,
             cod_seg_tipo_seguro,
             ds_seg_tipo_seguro,
             cod_seg_ramo,
             ds_seg_ramo,
             cod_seg_producto,
             ds_seg_producto,
             cod_seg_plan,
             ds_seg_plan,
             cod_seg_clase,
             ds_seg_clase,
             cod_seg_zona,
             cod_seg_sucursal,
             ds_seg_sucursal,
             cod_seg_canal,
             ds_seg_canal,
             ds_seg_agrupador_canales,
             cod_seg_tipo_mercado,
             ds_seg_tipo_mercado,
             dt_seg_fecha_operacion,
             dt_seg_fecha_alta,
             dt_seg_fecha_alta_poliza_original,
             cod_seg_operacion,
             ds_seg_operacion,
             cod_seg_usuario,
             ds_seg_tipo_doc,
             ds_seg_num_doc,
             ds_per_nombre,
             fc_seg_suma_asegurada,
             fc_seg_premio,
             fc_seg_prima,
             fc_seg_comision,
             cod_seg_tipo_cuenta,
             ds_seg_tipo_cuenta,
             cod_seg_sucursal_cuenta,
             cod_seg_cuenta,
             ds_seg_compania,
             cod_seg_familia,
             ds_seg_familia,
             cod_seg_anulacion,
             ds_seg_anulacion,
             partition_date
        from TMP_SEG_BAJAS TSB
        where not exists (select 1
                        from TMP_NOT_EXISTS1 TMPNE1
                        where TMPNE1.cace_carp_cd_ramo = TSB.cod_seg_ramo
                          and TMPNE1.cace_nu_poliza_original = TSB.cace_nu_poliza_original
                          and TMPNE1.caod_fe_operacion = TSB.caod_fe_operacion)
    UNION ALL
    select   cod_per_nup,
             cod_seg_poliza,
             cod_seg_certificado,
             cod_seg_tipo_seguro,
             ds_seg_tipo_seguro,
             cod_seg_ramo,
             ds_seg_ramo,
             cod_seg_producto,
             ds_seg_producto,
             cod_seg_plan,
             ds_seg_plan,
             cod_seg_clase,
             ds_seg_clase,
             cod_seg_zona,
             cod_seg_sucursal,
             ds_seg_sucursal,
             cod_seg_canal,
             ds_seg_canal,
             ds_seg_agrupador_canales,
             cod_seg_tipo_mercado,
             ds_seg_tipo_mercado,
             dt_seg_fecha_operacion,
             dt_seg_fecha_alta,
             dt_seg_fecha_alta_poliza_original,
             cod_seg_operacion,
             ds_seg_operacion,
             cod_seg_usuario,
             ds_seg_tipo_doc,
             ds_seg_num_doc,
             ds_per_nombre,
             fc_seg_suma_asegurada,
             fc_seg_premio,
             fc_seg_prima,
             fc_seg_comision,
             cod_seg_tipo_cuenta,
             ds_seg_tipo_cuenta,
             cod_seg_sucursal_cuenta,
             cod_seg_cuenta,
             ds_seg_compania,
             cod_seg_familia,
             ds_seg_familia,
             cod_seg_anulacion,
             ds_seg_anulacion,
             partition_date
        from TMP_SEG_BAJAS TSB,
             TMP_EXISTS_ROWID TMPNE1
       where TSB.cod_seg_ramo = TMPNE1.cace_carp_cd_ramo
         and TSB.cace_nu_poliza_original = TMPNE1.cace_nu_poliza_original
         and TSB.dt_seg_fecha_operacion = TMPNE1.caod_fe_operacion
         and TSB.cod_seg_rowid = TMPNE1.max_caod_rowid
) A;