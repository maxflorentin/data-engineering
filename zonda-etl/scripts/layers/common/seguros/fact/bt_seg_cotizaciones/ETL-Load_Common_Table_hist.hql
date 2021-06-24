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
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}';

--armo tabla temporal TEMP_RAMOS_PRODUCTOS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_RAMOS_PRODUCTOS as
SELECT  RV_LOW_VALUE,
        RV_HIGH_VALUE,
        RV_TYPE
FROM bi_corp_staging.rio6_CG_REF_CODES
WHERE RV_DOMAIN = 'TABLEAU.RAMOS_PRODUCTOS'
and partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'<'2020-10-20','2020-10-20','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}');

--armo tabla temporal TEMP_TIPO_MERCADO
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_TIPO_MERCADO as
SELECT  RV_LOW_VALUE,
        RV_HIGH_VALUE,
        RV_TYPE
FROM bi_corp_staging.rio6_CG_REF_CODES
WHERE RV_DOMAIN = 'TABLEAU.TIPO_MERCADO'
and partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'<'2020-11-18','2020-11-18','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}');

--------------Calculo la maxima particion PEDT001 ya que la misma se carga dias habiles
CREATE TEMPORARY TABLE tmp_maxpart001 AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.malpe_pedt001
 WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}',7)
  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}';

CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_PEDT001 as
select cast(p1.penumper as bigint) penumper,
	   case when trim(p1.pepriape)=''
	   	then trim(p1.penomfan)
	   		else
	   			trim(concat(trim(p1.pepriape),' ',trim(p1.penomper)))
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

-- Inserto en la tabla bt_seg_cotizaciones
INSERT OVERWRITE TABLE bi_corp_common.bt_seg_cotizaciones
partition(partition_date)
  select  CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
        CAST(CAZB.cazb_capo_nu_poliza AS BIGINT) cod_seg_poliza,
        CAST(CAZB.cazb_cace_nu_certificado AS BIGINT) cod_seg_certificado,
        TS.cod_seg_tipo_seguro,
        coalesce(TS.ds_seg_tipo_seguro,
                (CASE WHEN TRIM(TRP.rv_low_value) = 'VIDA CON AHORRO' then 'VIDA' else TRIM(TRP.rv_low_value) end),
                (CASE WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40 then 'AUTOS'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and TRIM(CACE.cace_capu_cd_producto) like '%PF%' then 'PROTECCION FEMENINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and SUBSTRING(TRIM(CACE.cace_capu_cd_producto),1,2) = 'PM' then 'PROTECCION MASCULINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) = 68 then 'OTROS' end)
               ) ds_seg_tipo_seguro,
        CAST(CAZB.cazb_carp_cd_ramo AS BIGINT) cod_seg_ramo,
        TS.ds_seg_ramo ds_seg_ramo,
        TRIM(CAZB.cazb_capu_cd_producto) cod_seg_producto,
        TS.ds_seg_producto ds_seg_producto,
        coalesce(TRIM(CACE.cace_capb_cd_plan),TRIM(CAZB.cazb_capb_cd_plan)) cod_seg_plan,
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
        from_unixtime(UNIX_TIMESTAMP(case when trim(CAZB.cazb_fe_cotizacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CAZB.cazb_fe_cotizacion) end),'yyyy-MM-dd') dt_seg_fecha_cotizacion,
        from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_desde) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_desde) end),'yyyy-MM-dd') dt_seg_fecha_vigencia,
        CAST(CAZB.cazb_nu_cotizacion AS BIGINT) cod_seg_cotizacion,
        TRIM(CAZB.cazb_cd_funcionario_banc) cod_seg_usuario,
        CASE WHEN TRIM(CAZB.cazb_cacn_cd_nacionalidad) = 'null' then NULL ELSE TRIM(CAZB.cazb_cacn_cd_nacionalidad) END ds_seg_tipo_doc,
        CAST(CAZB.cazb_cacn_cd_cliente AS BIGINT) ds_seg_num_doc,
        CASE WHEN TPDT01.penomper IS NULL THEN TRIM(CACN.cacn_nm_apellido_razon) ELSE TPDT01.penomper END ds_per_nombre,
        COALESCE(CASE WHEN TRIM(CACE.cace_mt_saldo_deudor) is null or TRIM(CACE.cace_mt_saldo_deudor) = 'null' THEN CAST(CACE.cace_mt_suma_asegurada AS bigint) ELSE CAST(CACE.cace_mt_saldo_deudor AS bigint) END,CAST(CAZB.CAZB_MT_SUMA_ASEGURADA AS BIGINT)) fc_seg_suma_asegurada,
        COALESCE(
        CAST(case
              when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))
        end AS DECIMAL (15,2)),CAST(CAZB.CAZB_MT_PREMIO as decimal(13,2))) fc_seg_premio,
        CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))
        end AS DECIMAL(15,2)) fc_seg_prima,
        CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))
        end AS DECIMAL(15,2)) fc_seg_comision,
        TRIM(CAFP.cafp_cd_fr_pago) cod_seg_forma_pago,
        TRIM(CAFP.cafp_de_fr_pago) ds_seg_forma_pago,
        CAST(CAZB.CAZB_MT_PAGO_INICIAL AS DECIMAL(12,2)) fc_seg_pago_inicial,
        CAST(CAZB.CAZB_MT_PAGO_SIGUIENTE AS DECIMAL(12,2)) fc_seg_pago_siguiente,
        TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
        TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
        TRIM(CACM.cacm_de_compania) ds_seg_compania,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
        CAST(CAZB.CAZB_TP_TASACION AS BIGINT) cod_seg_tipo_tasacion,
        CAST(CAZB.CAZB_AUPZ_CD_ZONA AS BIGINT) cod_seg_cazb_zona,
        TRIM(CAZB.CAZB_CAES_CD_CIUDAD) cod_seg_ciudad,
        TRIM(caci.caci_de_ciudad) ds_seg_ciudad,
        CASE WHEN TRIM(CAZB.CAZB_CAZP_CD_POSTAL) = 'null' then null else TRIM(CAZB.CAZB_CAZP_CD_POSTAL) end ds_seg_cod_postal,
        CAST(CASE TRIM(CAZB.CAZB_CAPB_IN_INSPECCION) WHEN 'S' THEN 1
           WHEN 'N' THEN 0
           ELSE NULL
        END AS BIGINT) flag_seg_inspeccion,
        CAST(CAZB.CAZB_CAMF_NU_FLOTA AS BIGINT) cod_seg_numero_flota,
        CASE WHEN TRIM(CAZB.CAZB_CD_TIPO_ACCESORIO) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_CD_TIPO_ACCESORIO) END cod_seg_tipo_accesorio,
        CAST(CAZB.CAZB_MT_SUMA_ASEG_ACCESORIO AS BIGINT) ft_seg_suma_asegurada_accesorio,
        CASE WHEN TRIM(CAZB.CAZB_AUMA_CD_MARCA) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_AUMA_CD_MARCA) END cod_seg_marca,
        NULL ds_seg_marca,
        CAST(CAZB.CAZB_AUMO_CD_MODELO AS BIGINT) cod_seg_modelo,
        NULL ds_seg_modelo,
        CAST(CAZB.CAZB_ANO_VEHICULO AS BIGINT) int_seg_ano_fabricacion,
        CAST(CAZB_NU_PRESUPUESTO_CIA AS BIGINT) cod_seg_numero_presupuesto_cia,
        CAST(CASE TRIM(CAZB.CAZB_IN_0KM) WHEN 'S' THEN 1
          WHEN 'N' THEN 0
          ELSE NULL END AS BIGINT) flag_seg_0km,
        CAST(CAZB.CAZB_CEMA_CD_MARCA AS BIGINT) cod_seg_marca_cel,
        CASE WHEN TRIM(CEMA.CEMA_DE_MARCA) = 'null' THEN NULL ELSE TRIM(CEMA.CEMA_DE_MARCA) END  ds_seg_marca_cel,
        CAST(CAZB.CAZB_CEMO_CD_MODELO AS BIGINT) cod_seg_modelo_cel,
        CASE WHEN TRIM(CEMO.CEMO_CD_MODELO_COMERCIAL) = 'null' THEN NULL ELSE TRIM(CEMO.CEMO_CD_MODELO_COMERCIAL) END ds_seg_modelo_cel,
        CASE WHEN TRIM(CAZB.CAZB_ID_DISPOSITIVO_MOVIL) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_ID_DISPOSITIVO_MOVIL) END cod_seg_imei_cel,
        CASE WHEN TRIM(CAZB.CAZB_IN_RIESGO_CLIENTE) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_IN_RIESGO_CLIENTE) END cod_seg_riesgo_cliente,
        CAST(CAZB.CAZB_MT_INGRESOS_CLIENTE AS BIGINT) fc_seg_ingreso_declarado_cliente,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}' partition_date
    from bi_corp_staging.rio6_cart_cotiza_banco CAZB
    LEFT JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAZB.cazb_capo_cd_sucursal = CACE.cace_casu_cd_sucursal and
            CAZB.cazb_carp_cd_ramo = CACE.cace_carp_cd_ramo and
            CAZB.cazb_capo_nu_poliza = CACE.cace_capo_nu_poliza and
            CAZB.cazb_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_celt_modelos CEMO
	    ON (CEMO.CEMO_CEMA_CD_MARCA = CAZB.CAZB_CEMA_CD_MARCA and
	        CEMO.CEMO_CD_MODELO     = CAZB.CAZB_CEMO_CD_MODELO and
	        CEMO.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
	LEFT JOIN bi_corp_staging.rio6_celt_marcas CEMA
	    ON (CEMA.CEMA_CD_MARCA = CAZB.CAZB_CEMA_CD_MARCA and
	        CEMA.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_ciudades CACI
        ON (CACI.caci_caes_cd_estado = CAZB.CAZB_CAES_CD_ESTADO and
            CACI.caci_cd_ciudad = CAZB.CAZB_CAES_CD_CIUDAD and
            CACI.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
           )
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CAZB.cazb_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPU.capu_cd_producto and
            CAPU.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CAZB.cazb_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CAZB.cazb_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAZB.cazb_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_clientes CACN
        ON (CAZB.cazb_cacn_cd_nacionalidad = CACN.cacn_cd_nacionalidad and
            CAZB.cazb_cacn_cd_cliente   = CACN.cacn_nu_cedula_rif and
            CACN.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_PEDT001 TPDT01
        ON (CAZB.cazb_cacn_cd_nacionalidad = TPDT01.petipdoc2 and
            CAST(CAZB.cazb_cacn_cd_cliente AS BIGINT) = TPDT01.penumdoc and
            TRIM(CACN.cacn_cd_sexo) = TPDT01.pesexper and
            to_date(CACN.cacn_fe_nacimiento) = TPDT01.pefecnac)
    LEFT JOIN bi_corp_staging.rio6_cart_domicilios_bancarios CADM
        ON (CACE.cace_cacn_cd_nacionalidad = CADM.cadm_cacn_cd_nacionalidad and
            CACE.cace_cacn_nu_cedula_rif   = CADM.cadm_cacn_nu_cedula_rif and
            CACE.cace_cadm_nu_domicilio_db = CADM.cadm_nu_domicilio and
            CADM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_TP_CUENTAS TTC
        ON (CADM.cadm_tp_cuenta = TTC.ctcu_tp_cuenta)
    LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco CAPJ
        ON (CAZB.cazb_capj_cd_sucursal = CAPJ.capj_cd_sucursal and
            CAPJ.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_zona_sucursal CAZS
            ON (CACE.cace_cazb_cd_sucursal = CAZS.cazs_cd_sucursal and
                CAZS.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_canales  CANL
        ON (CAPJ.capj_canl_cd_canal = CANL.canl_cd_canal and
            CANL.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores_canales INGC
        ON (CANL.canl_cd_canal = INGC.ingc_inca_cd_canal and
            INGC.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores INAG
        ON (INGC.ingc_inag_cd_agrupador = INAG.inag_cd_agrupador and
            INAG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_TIPO_MERCADO TTM
            ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (coalesce(CAST(CAZB.cazb_carp_cd_ramo AS BIGINT),CAST(CACE.cace_carp_cd_ramo AS BIGINT))     = TS.cod_seg_ramo and
            coalesce(TRIM(CAZB.cazb_capu_cd_producto),TRIM(CACE.cace_capu_cd_producto)) = TS.cod_seg_producto and
            coalesce(TRIM(CAZB.cazb_capb_cd_plan),TRIM(CACE.cace_capb_cd_plan))    = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_RAMOS_PRODUCTOS TRP
        ON (TRP.RV_HIGH_VALUE = CAPB.capb_carp_cd_ramo AND
            TRIM(TRP.rv_type)       = TS.cod_seg_tipo_seguro)
WHERE CAZB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
  AND CAST(CAZB.cazb_carp_cd_ramo AS BIGINT) not between 30 and 40
  AND TRIM(CAPU.capu_cacy_cd_clase) NOT IN ('HI','PE','PR','SD','TC','TR')
  AND NOT EXISTS  (select 1
                     from  bi_corp_staging.rio6_cart_operaciones_diarias caod
                     where caod.caod_casu_cd_sucursal    = cazb.cazb_capo_cd_sucursal
                       and caod.caod_carp_cd_ramo        = cazb.cazb_carp_cd_ramo
                       and caod.caod_capo_nu_poliza      = cazb.cazb_capo_nu_poliza
                       and caod.caod_cace_nu_certificado = cazb.cazb_cace_nu_certificado
                       and caod.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
                       and caod.caod_caop_cd_operacion   = '1006'
                       and caod.caod_cd_sub_operacion    = '1')
UNION ALL
select  CASE WHEN TPDT01.penumper IS NULL THEN CAST(CACN.cacn_nu_nup AS BIGINT) ELSE TPDT01.penumper END cod_per_nup,
        CAST(CAZB.cazb_capo_nu_poliza AS BIGINT) cod_seg_poliza,
        CAST(CAZB.cazb_cace_nu_certificado AS BIGINT) cod_seg_certificado,
        TS.cod_seg_tipo_seguro,
        coalesce(TS.ds_seg_tipo_seguro,'AUTOS') ds_seg_tipo_seguro,
        CAST(CAZB.cazb_carp_cd_ramo AS BIGINT) cod_seg_ramo,
        TS.ds_seg_ramo ds_seg_ramo,
        TRIM(CAZB.cazb_capu_cd_producto) cod_seg_producto,
        TS.ds_seg_producto ds_seg_producto,
        coalesce(TRIM(CACE.cace_capb_cd_plan),TRIM(CAZB.cazb_capb_cd_plan)) cod_seg_plan,
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
        from_unixtime(UNIX_TIMESTAMP(case when trim(CAZB.cazb_fe_cotizacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CAZB.cazb_fe_cotizacion) end),'yyyy-MM-dd') dt_seg_fecha_cotizacion,
        from_unixtime(UNIX_TIMESTAMP(case when trim(CACE.cace_fe_desde) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(CACE.cace_fe_desde) end),'yyyy-MM-dd') dt_seg_fecha_vigencia,
        CAST(CAZB.cazb_nu_cotizacion AS BIGINT) cod_seg_cotizacion,
        TRIM(CAZB.cazb_cd_funcionario_banc) cod_seg_usuario,
        CASE WHEN TRIM(CAZB.cazb_cacn_cd_nacionalidad) = 'null' then NULL ELSE TRIM(CAZB.cazb_cacn_cd_nacionalidad) END ds_seg_tipo_doc,
        CAST(CAZB.cazb_cacn_cd_cliente AS BIGINT) ds_seg_num_doc,
        CASE WHEN TPDT01.penomper IS NULL THEN TRIM(CACN.cacn_nm_apellido_razon) ELSE TPDT01.penomper END ds_per_nombre,
        COALESCE(CASE WHEN TRIM(CACE.cace_mt_saldo_deudor) is null or TRIM(CACE.cace_mt_saldo_deudor) = 'null' THEN CAST(CACE.cace_mt_suma_asegurada AS bigint) ELSE CAST(CACE.cace_mt_saldo_deudor AS bigint) END,CAST(CAZB.CAZB_MT_SUMA_ASEGURADA AS BIGINT)) fc_seg_suma_asegurada,
        COALESCE(
        CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_anual as decimal(15,2)))
        end AS DECIMAL (15,2)),CAST(CAZB.CAZB_MT_PREMIO as decimal(13,2))) fc_seg_premio,
        CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_prima_pura as decimal(15,2)))
        end AS DECIMAL(15,2)) fc_seg_prima,
        CAST(case
            when TRIM(CAPU.capu_in_fact_adel) = 'S' then (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))/(CASE WHEN CAFP.cafp_nu_cuotas_cobrar is null THEN 12 ELSE CAST(CAFP.cafp_nu_cuotas_cobrar AS BIGINT) END)
            else (CAST(CACE.cace_mt_comision_anual as decimal(15,2)))
        end AS DECIMAL(15,2)) fc_seg_comision,
        TRIM(CAFP.cafp_cd_fr_pago) cod_seg_forma_pago,
        TRIM(CAFP.cafp_de_fr_pago) ds_seg_forma_pago,
        CAST(CAZB.CAZB_MT_PAGO_INICIAL AS DECIMAL(12,2)) fc_seg_pago_inicial,
        CAST(CAZB.CAZB_MT_PAGO_SIGUIENTE AS DECIMAL(12,2)) fc_seg_pago_siguiente,
        TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
        TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
        TRIM(CACM.cacm_de_compania) ds_seg_compania,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
        CAST(CAZB.CAZB_TP_TASACION AS BIGINT) cod_seg_tipo_tasacion,
        CAST(CAZB.CAZB_AUPZ_CD_ZONA AS BIGINT) cod_seg_cazb_zona,
        CAST(CAZB.CAZB_CAES_CD_CIUDAD AS BIGINT) cod_seg_ciudad,
        TRIM(caci.caci_de_ciudad) ds_seg_ciudad,
        CASE WHEN TRIM(CAZB.CAZB_CAZP_CD_POSTAL) = 'null' then null else TRIM(CAZB.CAZB_CAZP_CD_POSTAL) end ds_seg_cod_postal,
        CAST(CASE TRIM(CAZB.CAZB_CAPB_IN_INSPECCION) WHEN 'S' THEN 1
             WHEN 'N' THEN 0
             ELSE NULL
        END AS BIGINT) flag_seg_inspeccion,
        CAST(CAZB.CAZB_CAMF_NU_FLOTA AS BIGINT) cod_seg_numero_flota,
        CASE WHEN TRIM(CAZB.CAZB_CD_TIPO_ACCESORIO) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_CD_TIPO_ACCESORIO) END cod_seg_tipo_accesorio,
        CAST(CAZB.CAZB_MT_SUMA_ASEG_ACCESORIO AS BIGINT) ft_seg_suma_asegurada_accesorio,
        CASE WHEN TRIM(CAZB.CAZB_AUMA_CD_MARCA) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_AUMA_CD_MARCA) END cod_seg_marca,
        CASE WHEN TRIM(AUMA.AUMA_DE_MARCA) = 'null' THEN NULL ELSE TRIM(AUMA.AUMA_DE_MARCA) END ds_seg_marca,
        CAST(CAZB.CAZB_AUMO_CD_MODELO AS BIGINT) cod_seg_modelo,
        CASE WHEN TRIM(AUMO.AUMO_DE_MODELO) = 'null' THEN NULL ELSE TRIM(AUMO.AUMO_DE_MODELO) END ds_seg_modelo,
        CAST(CAZB.CAZB_ANO_VEHICULO AS BIGINT) int_seg_ano_fabricacion,
        CAST(CAZB_NU_PRESUPUESTO_CIA AS BIGINT) cod_seg_numero_presupuesto_cia,
        CAST(CASE TRIM(CAZB.CAZB_IN_0KM) WHEN 'S' THEN 1
                                    WHEN 'N' THEN 0
                                    ELSE NULL END AS BIGINT) flag_seg_0km,
        CAST(CAZB.CAZB_CEMA_CD_MARCA AS BIGINT) cod_seg_marca_cel,
        NULL ds_seg_marca_cel,
        CAST(CAZB.CAZB_CEMO_CD_MODELO AS BIGINT) cod_seg_modelo_cel,
        NULL ds_seg_modelo_cel,
        CASE WHEN TRIM(CAZB.CAZB_ID_DISPOSITIVO_MOVIL) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_ID_DISPOSITIVO_MOVIL) END cod_seg_imei_cel,
        CASE WHEN TRIM(CAZB.CAZB_IN_RIESGO_CLIENTE) = 'null' THEN NULL ELSE TRIM(CAZB.CAZB_IN_RIESGO_CLIENTE) END cod_seg_riesgo_cliente,
        CAST(CAZB.CAZB_MT_INGRESOS_CLIENTE AS BIGINT) fc_seg_ingreso_declarado_cliente,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}' partition_date
    from bi_corp_staging.rio6_cart_cotiza_banco CAZB
    INNER JOIN bi_corp_staging.rio6_autt_modelos AUMO
        ON (CAZB.cazb_auma_cd_marca = AUMO.aumo_auma_cd_marca and
            CAZB.cazb_aumo_cd_modelo = AUMO.aumo_cd_modelo and
            AUMO.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_autt_marcas AUMA
        ON (AUMO.aumo_auma_cd_marca = AUMA.auma_cd_marca and
            AUMA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAZB.cazb_capo_cd_sucursal = CACE.cace_casu_cd_sucursal and
            CAZB.cazb_carp_cd_ramo = CACE.cace_carp_cd_ramo and
            CAZB.cazb_capo_nu_poliza = CACE.cace_capo_nu_poliza and
            CAZB.cazb_cace_nu_certificado = CACE.cace_nu_certificado and
            CAZB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}' and
            CACE.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_ciudades CACI
        ON (CACI.caci_caes_cd_estado = CAZB.CAZB_CAES_CD_ESTADO and
            CACI.caci_cd_ciudad = CAZB.CAZB_CAES_CD_CIUDAD and
            CACI.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
           )
    LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
        ON (CAZB.cazb_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPU.capu_cd_producto and
            CAPU.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
        ON (CAZB.cazb_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CAZB.cazb_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAZB.cazb_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAZB.cazb_capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_companias CACM
        ON (CAPB.capb_cacm_cd_compania = CACM.cacm_cd_compania and
            CACM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_clientes CACN
        ON (CAZB.cazb_cacn_cd_nacionalidad = CACN.cacn_cd_nacionalidad and
            CAZB.cazb_cacn_cd_cliente   = CACN.cacn_nu_cedula_rif and
            CACN.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_PEDT001 TPDT01
        ON (CAZB.cazb_cacn_cd_nacionalidad = TPDT01.petipdoc2 and
            CAST(CAZB.cazb_cacn_cd_cliente AS BIGINT) = TPDT01.penumdoc and
            TRIM(CACN.cacn_cd_sexo) = TPDT01.pesexper and
            to_date(CACN.cacn_fe_nacimiento) = TPDT01.pefecnac)
    LEFT JOIN bi_corp_staging.rio6_cart_domicilios_bancarios CADM
        ON (CACE.cace_cacn_cd_nacionalidad = CADM.cadm_cacn_cd_nacionalidad and
            CACE.cace_cacn_nu_cedula_rif   = CADM.cadm_cacn_nu_cedula_rif and
            CACE.cace_cadm_nu_domicilio_db = CADM.cadm_nu_domicilio and
            CADM.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_TP_CUENTAS TTC
        ON (CADM.cadm_tp_cuenta = TTC.ctcu_tp_cuenta)
    LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco CAPJ
        ON (CAZB.cazb_capj_cd_sucursal = CAPJ.capj_cd_sucursal and
            CAPJ.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_zona_sucursal CAZS
            ON (CACE.cace_cazb_cd_sucursal = CAZS.cazs_cd_sucursal and
                CAZS.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_canales  CANL
        ON (CAPJ.capj_canl_cd_canal = CANL.canl_cd_canal and
            CANL.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores_canales INGC
        ON (CANL.canl_cd_canal = INGC.ingc_inca_cd_canal and
            INGC.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN bi_corp_staging.rio6_inft2_agrupadores INAG
        ON (INGC.ingc_inag_cd_agrupador = INAG.inag_cd_agrupador and
            INAG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_TIPO_MERCADO TTM
            ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
    LEFT JOIN bi_corp_common.dim_seg_tipo_seguro TS
        ON (coalesce(CAST(CAZB.cazb_carp_cd_ramo AS BIGINT),CAST(CACE.cace_carp_cd_ramo AS BIGINT))     = TS.cod_seg_ramo and
            coalesce(TRIM(CAZB.cazb_capu_cd_producto),TRIM(CACE.cace_capu_cd_producto)) = TS.cod_seg_producto and
            coalesce(TRIM(CAZB.cazb_capb_cd_plan),TRIM(CACE.cace_capb_cd_plan))    = TS.cod_seg_plan and
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}')
    LEFT JOIN TEMP_RAMOS_PRODUCTOS TRP
        ON (TRP.RV_HIGH_VALUE = CAPB.capb_carp_cd_ramo AND
            TRIM(TRP.rv_type)       = TS.cod_seg_tipo_seguro)
WHERE CAZB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
  AND CAST(CAZB.cazb_carp_cd_ramo AS BIGINT) between 30 and 40
  AND TRIM(CAPU.capu_cacy_cd_clase) NOT IN ('HI','PE','PR','SD','TC','TR')
  AND NOT EXISTS  (select 1
                     from  bi_corp_staging.rio6_cart_operaciones_diarias caod
                     where caod.caod_casu_cd_sucursal    = cazb.cazb_capo_cd_sucursal
                       and caod.caod_carp_cd_ramo        = cazb.cazb_carp_cd_ramo
                       and caod.caod_capo_nu_poliza      = cazb.cazb_capo_nu_poliza
                       and caod.caod_cace_nu_certificado = cazb.cazb_cace_nu_certificado
                       and caod.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros_hist') }}'
                       and caod.caod_caop_cd_operacion   = '1006'
                       and caod.caod_cd_sub_operacion    = '1');