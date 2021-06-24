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

--armo tabla temporal TEMP_RAMOS_PRODUCTOS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_RAMOS_PRODUCTOS as
SELECT  RV_LOW_VALUE,
        RV_HIGH_VALUE,
        RV_TYPE
FROM bi_corp_staging.rio6_CG_REF_CODES
WHERE RV_DOMAIN = 'TABLEAU.RAMOS_PRODUCTOS'
and partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'<'2020-10-20','2020-10-20','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}');

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

--armo tabla temporal TEMP_STK_SEG_SINIESTROS
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_STK_SEG_SINIESTROS as
    select	CAST(CACN.cacn_nu_nup AS BIGINT) cod_per_nup,
    		CAST(csai.csai_capo_nu_poliza AS BIGINT) cod_seg_poliza,
            CAST(csai.csai_cace_nu_certificado AS BIGINT) cod_seg_certificado,
            TS.cod_seg_tipo_seguro,
            coalesce(TS.ds_seg_tipo_seguro,
                (CASE WHEN TRIM(TRP.rv_low_value) = 'VIDA CON AHORRO' then 'VIDA' else TRIM(TRP.rv_low_value) end),
                (CASE WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40 then 'AUTOS'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and TRIM(CACE.cace_capu_cd_producto) like '%PF%' then 'PROTECCION FEMENINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (24,59) and SUBSTRING(TRIM(CACE.cace_capu_cd_producto),1,2) = 'PM' then 'PROTECCION MASCULINA'
		              WHEN CAST(CACE.cace_carp_cd_ramo AS BIGINT) = 68 then 'OTROS' end)
               ) ds_seg_tipo_seguro,
            coalesce(CAST(CACE.cace_carp_cd_ramo AS BIGINT),CAST(csai.csai_carp_cd_ramo AS BIGINT)) cod_seg_ramo,
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
       			from_unixtime(UNIX_TIMESTAMP(case when trim(csai.csai_fe_registracion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(csai.csai_fe_registracion) end),'yyyy-MM-dd') dt_seg_fecha_registracion,
            from_unixtime(UNIX_TIMESTAMP(case when trim(csai.csai_fe_siniestro) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(csai.csai_fe_siniestro) end),'yyyy-MM-dd') dt_seg_fecha_siniestro,
            from_unixtime(UNIX_TIMESTAMP(case when trim(csai.csai_fe_comunicacion) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(csai.csai_fe_comunicacion) end),'yyyy-MM-dd') dt_seg_fecha_comunicacion,
    				from_unixtime(UNIX_TIMESTAMP(case when trim(csai.csai_cace_fe_desde) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(csai.csai_cace_fe_desde) end),'yyyy-MM-dd') dt_seg_fecha_desde_vigencia,
            from_unixtime(UNIX_TIMESTAMP(case when trim(csai.csai_cace_fe_hasta) in ('9999-12-31', '0001-01-01','0000-00-00') then null else trim(csai.csai_cace_fe_hasta) end),'yyyy-MM-dd') dt_seg_fecha_hasta_vigencia,
            CAST(csai.csai_nu_orden AS BIGINT) cod_seg_orden,
            CAST(csai.csai_nu_siniestro AS BIGINT) cod_seg_siniestro,
            TRIM(cg.rv_high_value) cod_seg_estado,
            TRIM(csai.csai_tp_documento) ds_seg_tipo_doc,
            CAST(csai.csai_nu_documento AS BIGINT) ds_seg_num_doc,
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
			      TRIM(csai.csai_nm_aseg) ds_per_asegurado,
			      TRIM(CAFP.cafp_cd_fr_pago) cod_seg_forma_pago,
		        TRIM(CAFP.cafp_de_fr_pago) ds_seg_forma_pago,
		        TRIM(ttc.ctcu_in_cuenta) cod_seg_tipo_cuenta,
		        TRIM(ttc.ctcu_nm_cuenta) ds_seg_tipo_cuenta,
		        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),1,3) AS BIGINT) ELSE NULL END cod_seg_sucursal_cuenta,
		        CASE WHEN TRIM(ttc.ctcu_in_cuenta) = 'C' THEN CAST(SUBSTRING(TRIM(CADM.cadm_nu_cuenta),4) AS BIGINT) ELSE CAST(CADM.cadm_nu_cuenta AS BIGINT) END cod_seg_cuenta,
		        TRIM(CAES.caes_de_estado) ds_seg_provincia,
		        CAST(CACI.caci_cd_ciudad AS BIGINT) cod_seg_ciudad,
		        TRIM(caci.caci_de_ciudad) ds_seg_ciudad,
		        CASE WHEN trim(CACE.cace_zn_postal) = 'null' then null else trim(CACE.cace_zn_postal) end ds_seg_cod_postal,
		        CASE WHEN TRIM(csai.csai_nm_beneficiario1) = 'null' THEN NULL ELSE TRIM(csai.csai_nm_beneficiario1) END ds_seg_beneficiario1,
            CASE WHEN TRIM(csai.csai_nm_beneficiario2) = 'null' THEN NULL ELSE TRIM(csai.csai_nm_beneficiario2) END ds_seg_beneficiario2,
            CASE WHEN TRIM(csai.csai_nm_beneficiario3) = 'null' THEN NULL ELSE TRIM(csai.csai_nm_beneficiario3) END ds_seg_beneficiario3,
            CASE WHEN TRIM(csai.csai_nm_beneficiario4) = 'null' THEN NULL ELSE TRIM(csai.csai_nm_beneficiario4) END ds_seg_beneficiario4,
            CASE WHEN TRIM(csai.csai_nm_beneficiario5) = 'null' THEN NULL ELSE TRIM(csai.csai_nm_beneficiario5) END ds_seg_beneficiario5,
            CONCAT(TRIM(cacm.cacm_nu_apartado),' - ',TRIM(cacm.cacm_de_compania)) ds_seg_compania_aseguradora,
            TRIM(csai.csai_de_siniestro) ds_seg_observacion_siniestro,
            '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' partition_date
    from bi_corp_staging.rio6_cart_siniestros_age_inst csai
    inner join bi_corp_staging.rio6_cg_ref_codes cg
      on (csai.csai_st_siniestro = cg.rv_low_value and
          csai.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
          cg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    inner join bi_corp_staging.rio6_cart_companias cacm
      on (csai.csai_cd_compania = cacm.cacm_cd_compania and
          cacm.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    left join bi_corp_staging.rio6_cart_certificados CACE
      on (csai.csai_carp_cd_ramo = cace.cace_carp_cd_ramo and
          csai.csai_capo_nu_poliza = cace.cace_capo_nu_poliza and
          csai.csai_cace_nu_certificado = cace.cace_nu_certificado and
          CACE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
         )
	LEFT JOIN bi_corp_staging.rio6_cart_productos CAPU
	            ON (CACE.cace_carp_cd_ramo     = CAPU.capu_carp_cd_ramo and
	                CACE.cace_capu_cd_producto = CAPU.capu_cd_producto and
	                CAPU.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
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
            CAPB.partition_date        = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
            )
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos_productos CAPG
        ON (CAPU.capu_carp_cd_ramo = CAPG.capg_carp_cd_ramo and
            CAPU.capu_cd_producto = CAPG.capg_capu_cd_producto and
            CAPG.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_fr_pagos CAFP
        ON (CAPG.capg_cafp_cd_fr_pago = CAFP.cafp_cd_fr_pago and
            CAFP.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN bi_corp_staging.rio6_cart_clientes CACN
      ON (COALESCE(TRIM(CACE.cace_cacn_cd_nacionalidad),TRIM(csai.csai_tp_documento)) = TRIM(CACN.cacn_cd_nacionalidad) and
          COALESCE(TRIM(CACE.cace_cacn_nu_cedula_rif),TRIM(csai.csai_nu_documento))  = TRIM(CACN.cacn_nu_cedula_rif) and
          CACN.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}')
    LEFT JOIN TEMP_PEDT001 TPDT01
        ON (COALESCE(TRIM(CACE.cace_cacn_cd_nacionalidad),TRIM(csai.csai_tp_documento)) = TPDT01.petipdoc2 and
            COALESCE(TRIM(CACE.cace_cacn_nu_cedula_rif),TRIM(csai.csai_nu_documento)) = TPDT01.penumdoc and
            TRIM(CACN.cacn_cd_sexo) = TPDT01.pesexper and
            to_date(CACN.cacn_fe_nacimiento) = TPDT01.pefecnac)
    LEFT JOIN bi_corp_staging.rio6_cart_domicilios_bancarios CADM
        ON (COALESCE(TRIM(CACE.cace_cacn_cd_nacionalidad),TRIM(csai.csai_tp_documento)) = TRIM(CADM.cadm_cacn_cd_nacionalidad) and
            COALESCE(TRIM(CACE.cace_cacn_nu_cedula_rif),TRIM(csai.csai_nu_documento))   = TRIM(CADM.cadm_cacn_nu_cedula_rif) and
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
            TS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}' and
            CACE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Seguros') }}'
            )
	LEFT JOIN TEMP_RAMOS_PRODUCTOS TRP
        ON (TRP.RV_HIGH_VALUE = CAPB.capb_carp_cd_ramo AND
            TRIM(TRP.rv_type)       = TS.cod_seg_tipo_seguro)
	LEFT JOIN TEMP_TIPO_MERCADO TTM
        			ON (TTM.rv_low_value = CAPU.capu_cacy_cd_clase)
    where cg.rv_domain = 'SINT_SINIESTROS.SISI_ST_SINIESTRO';

-- Inserto en la tabla stk_seg_siniestros con fuente de SSN
INSERT OVERWRITE TABLE bi_corp_common.stk_seg_siniestros
partition(partition_date)
select  COALESCE(p.penumper,t.cod_per_nup)  cod_per_nup,
        t.cod_seg_poliza,
        t.cod_seg_certificado,
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
        t.dt_seg_fecha_registracion,
        t.dt_seg_fecha_siniestro,
        t.dt_seg_fecha_comunicacion,
        t.dt_seg_fecha_desde_vigencia,
        t.dt_seg_fecha_hasta_vigencia,
        t.cod_seg_orden,
        t.cod_seg_siniestro,
        t.cod_seg_estado,
        t.ds_seg_tipo_doc,
        t.ds_seg_num_doc,
        t.ds_per_nombre,
        t.fc_seg_suma_asegurada,
        t.fc_seg_premio,
        t.fc_seg_prima,
        t.fc_seg_comision,
        t.ds_per_asegurado,
        t.cod_seg_forma_pago,
        t.ds_seg_forma_pago,
        t.cod_seg_tipo_cuenta,
        t.ds_seg_tipo_cuenta,
        t.cod_seg_sucursal_cuenta,
        t.cod_seg_cuenta,
        t.ds_seg_provincia,
        t.cod_seg_ciudad,
        t.ds_seg_ciudad,
        t.ds_seg_cod_postal,
        t.ds_seg_beneficiario1,
        t.ds_seg_beneficiario2,
        t.ds_seg_beneficiario3,
        t.ds_seg_beneficiario4,
        t.ds_seg_beneficiario5,
        t.ds_seg_compania_aseguradora,
        t.ds_seg_observacion_siniestro,
        t.partition_date
from TEMP_STK_SEG_SINIESTROS t
left join  TEMP_PEDT016 p
on (t.cod_per_nup = p.penumper_viejo);