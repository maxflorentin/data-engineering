"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
---- TABLAS TEMPORALES-----

WITH IMP_LIM_GARANTIA AS (
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien, gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo desc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '003')esc where orden=1
),
VAB AS (
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien, gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo desc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '001')v where orden=1
),
VAB_ORI AS (
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien,gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo asc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '001')v where orden=1
),
HIP_UVA AS(
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien,gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo desc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '015')uva where orden=1
),
HIP_UVA_ORI AS (
select garantia, bien, divisa, valor_ori, fecha_alta
     from  (select row_number() OVER (partition by gttchis_his_num_garantia order by gttchis_his_num_sechisto asc) as orden,
      gttchis_his_num_garantia garantia,
	  CAST(TRIM(SUBSTRING(gttchis_his_des_historia,6,11)) AS BIGINT) bien,
      CAST(TRANSLATE(TRIM(SUBSTRING(gttchis_his_des_historia,29,18)), ",",".") AS decimal(19,4))  valor_ori,
      TRIM(SUBSTRING(gttchis_his_des_historia,47)) divisa,
      SUBSTRING(gttchis_his_timest_umo,1,10) fecha_alta
      from bi_corp_staging.gtdthis
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
      and  gttchis_his_tip_evento='021' ) hip_uva where orden=1
),
TASA AS(
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien,gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo desc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '002')tas where orden=1
),
TASA_ORI AS(
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien,gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo asc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '002')tas where orden=1
),
VAL_NOMINAL AS(
select gttcvab_vab_cod_divisa,gttcvab_vab_imp_total,gttcvab_vab_num_bien, gttcvab_vab_timest_umo
 from (select row_number() OVER (partition by gttcvab_vab_num_bien order by gttcvab_vab_timest_umo desc) as orden,
         gttcvab_vab_cod_divisa ,
 	 	 gttcvab_vab_imp_total,
 	     gttcvab_vab_num_bien,
 	     gttcvab_vab_timest_umo
  	  FROM bi_corp_staging.gtdtvab
      where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and gttcvab_vab_tip_valor = '014')val where orden=1
),
COTIZACIONES AS(
SELECT tcdt081_cddiviss	cod_divisa, partition_Date, (tcdt081_cambfix/100) as imp_cambio_fijo_vigente, concat(substr(tcdt081_fhcambio, 1,4), substr(tcdt081_fhcambio, 6,2), substr(tcdt081_fhcambio, 9,2)) as fec_cambio_yyyymmdd
				FROM bi_corp_staging.maltc_tcdt081
				WHERE partition_Date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
				and tcdt081_inddivbi = 'D' and tcdt081_indcot = 'S' and tcdt081_segmento = ''
                and tcdt081_fhcambio ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
),
UVA AS(
select tcdt090_valor uva , tcdt090_tcfechin , tcdt090_tcfechfi fecha
from bi_corp_staging.maltc_tcdt090
where tcdt090_coefici='UVA' and partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
order by tcdt090_tcfechin desc
),
UVA_ORI AS(
select tcdt090_valor uva_ori , tcdt090_tcfechin , tcdt090_tcfechfi fecha
from bi_corp_staging.maltc_tcdt090
where tcdt090_coefici='UVA' and partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'

), 
 PRESTAMOS AS(
 select cast(impconce as DECIMAL(19,4)) imp_deuda_original, oficina,cuenta,feforma fecha_alta_contrato
 from bi_corp_staging.malug_ugdtmae
 where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
 ),

 RGB AS (
 select gttcrgb_rgb_num_garantia,gttcrgb_rgb_num_bien, gttcrgb_rgb_fec_finvali,gttcrgb_rgb_fec_inivali,gttcrgb_rgb_por_biegaran
  from (select row_number() OVER (partition by gttcrgb_rgb_num_garantia, gttcrgb_rgb_num_bien order by gttcrgb_rgb_timest_umo desc) as orden,
          gttcrgb_rgb_num_garantia ,
  	 	 gttcrgb_rgb_num_bien,
  	     gttcrgb_rgb_fec_finvali,
  	     gttcrgb_rgb_fec_inivali,
  	     gttcrgb_rgb_por_biegaran
   	  FROM bi_corp_staging.gtdtrgb
       where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
       and gttcrgb_rgb_fec_finvali> '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS')}}') rg
       where orden=1
 ),
   RGB_NUP AS (
  select gttcrgb_rgb_num_garantia,gttcrgb_rgb_num_bien, gttcrgb_rgb_fec_finvali,gttcrgb_rgb_fec_inivali,gttcrgb_rgb_por_biegaran
   from (select row_number() OVER (partition by gttcrgb_rgb_num_garantia, gttcrgb_rgb_num_bien order by gttcrgb_rgb_timest_umo desc) as orden,
          gttcrgb_rgb_num_garantia ,
   	 	 gttcrgb_rgb_num_bien,
   	     gttcrgb_rgb_fec_finvali,
   	     gttcrgb_rgb_fec_inivali,
   	     gttcrgb_rgb_por_biegaran
    	  FROM bi_corp_staging.gtdtrgb
        where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}') rgn
        where orden=1
  ),


  RTG_PAIS AS(
  		SELECT P01.partition_date,P01.pecdgent, P01.penumper, P01.penumdoc, P01.PEPAIRES
 			,aux_garant_calif_pais.codigo_iso_pais codigo_iso_pais ,aux_garant_calif_pais.cal_unif_pais cal_unif_pais
 		FROM bi_corp_staging.malpe_pedt001 P01
 		INNER JOIN  bi_corp_bdr.aux_garant_calif_pais
 		ON  (P01.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
 		AND aux_garant_calif_pais.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_GARANTIAS') }}'
 		and p01.pepaires = aux_garant_calif_pais.pais)
 	),

     VAB_ORI_GAR AS (
     select   garantia,  valor_ori,    fecha_alta
     from ( select row_number() OVER (partition by gttchis_his_num_garantia order by gttchis_his_num_sechisto asc) as orden,
     		gttchis_his_num_garantia garantia,
     		CAST(TRANSLATE(TRANSLATE(TRANSLATE(TRIM(SUBSTRING(gttchis_his_des_historia,5,21)), ",","#"),".",""),"#",".") AS DECIMAL(19,4))  valor_ori ,
     		 SUBSTRING(gttchis_his_timest_umo,1,10) fecha_alta
     		from bi_corp_staging.gtdthis
            where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
     		and gttchis_his_tip_evento='015' ) gar_or where orden=1
     		),
 PROD_UVA AS(
 select codigo_producto,codigo_subproducto
 from  bi_corp_staging.risksql5_productos PA
 where fecha_informacion='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and pa.uva='true' and categoria_particulares='HIPOTECARIO'

 union all
  select codigo_producto,codigo_subproducto
  from  bi_corp_staging.risksql5_productos P
 where fecha_informacion='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and descripcion  LIKE '%CER%' and categoria_particulares='HIPOTECARIO')




                                                            -------- TABLA FISICA ---------

INSERT OVERWRITE TABLE bi_corp_common.stk_gar_garantias
PARTITION(partition_date)

-- GARANTIAS ESPECIFICAS
SELECT
distinct mae.gttcmae_mae_cod_entidad 																			cod_gar_entidad,
mae.gttcmae_mae_num_garantia 						       														cod_gar_num_garantia,
mae.gttcmae_mae_cod_garantia    																				cod_gar_garantia,
gar.gttcgar_gar_des_corta  																						ds_gar_garantia,
gar.gttcgar_gar_cla_garantia  																					cod_gar_cla_garantia,
gar.gttcgar_gar_des_larga																						ds_gar_cla_garantia,
mae.gttcmae_mae_tip_cobertur    																				cod_gar_tipo_cobertura,
'ESPECÍFICA'																									ds_gar_tipo_cobertura,
CAST(rbc.gttcrbc_rbc_num_persona  AS BIGINT) 																	cod_per_nup,
rgo.gttcrgo_rgo_tip_relacion 																					cod_gar_relacion,
CAST(rgo.gttcrgo_rgo_contrato_rgo_cod_oficont AS INT)															cod_gar_sucural,
CAST(rgo.gttcrgo_rgo_contrato_rgo_num_ctacont 	AS BIGINT)														cod_gar_num_cuenta,
rgo.gttcrgo_rgo_contrato_rgo_cod_producto																		cod_gar_producto,
rgo.gttcrgo_rgo_contrato_rgo_cod_subprod																		cod_gar_subproducto,
CASE WHEN mae.gttcmae_mae_fec_alta='0001-01-01' THEN NULL ELSE mae.gttcmae_mae_fec_alta END 					dt_gar_fecha_alta,
CASE WHEN mae.gttcmae_mae_fec_vcto='9999-12-31' THEN NULL ELSE mae.gttcmae_mae_fec_vcto END						dt_gar_fecha_vencimiento,
mae.gttcmae_mae_fec_activa																						dt_gar_fecha_activacion,
mae.gttcmae_mae_cod_divisa																						cod_gar_divisa,
IF (gar_ori.garantia IS NOT NULL,
    gar_ori.valor_ori,gttcmae_mae_imp_limite)																	fc_gar_importe_limiteoriginal,
CASE WHEN mae.gttcmae_mae_cod_divisa !='ARS'
	      		THEN CAST(mae.gttcmae_mae_imp_limite AS DECIMAL(19,4))*
	      		 CAST(cotizaciones_mae.imp_cambio_fijo_vigente AS DECIMAL(19,4))
         		ELSE CAST( mae.gttcmae_mae_imp_limite as DECIMAL(19,4)) END										fc_gar_importe_limiteactualizado,
mae.gttcmae_mae_fec_camlim																						dt_gar_fecha_cambiolimite,
CASE WHEN gttcmae_mae_ind_bancosor= 'S' THEN 1 ELSE 0 END 														flag_gar_bancosor,
mae.gttcmae_mae_cod_estado																						cod_gar_estado,
CASE WHEN mae.gttcmae_mae_cod_estado='010' THEN 'TRAMITE'
     WHEN mae.gttcmae_mae_cod_estado='020' THEN 'ACTIVO'
     WHEN mae.gttcmae_mae_cod_estado='030' THEN 'NO ACTIVO'
     ELSE NULL END                                                                                              ds_gar_estado,
mae.gttcmae_mae_cod_subestad																					cod_gar_subestado,
CAST(mae.gttcmae_mae_imp_disponib AS 	DECIMAL(19,4))															fc_gar_importe_disponible,
CAST(mae.gttcmae_mae_imp_alzado	AS 	DECIMAL(19,4))																fc_gar_importe_alzado,
CAST(mae.gttcmae_mae_imp_contable	AS DECIMAL(19,4))														 	fc_gar_importe_contable,
mae.gttcmae_mae_fec_ultcober																					dt_gar_fecha_ultimacobertura,
CASE WHEN gttcmae_mae_ind_bancosor= 'S' THEN 1 ELSE 0 END														flag_gar_reconstitucion,
rgo.gttcrgo_rgo_imp_cubierto																	 				fc_gar_contrato_valororiginal,
rgo.gttcrgo_rgo_contrato_rgo_cod_divcont 																		cod_gar_contrato_divisaoriginal,
CAST(CASE WHEN rgo.gttcrgo_rgo_contrato_rgo_cod_divcont !='ARS'
	      		THEN rgo.gttcrgo_rgo_imp_cubierto * cotizaciones_rgo.imp_cambio_fijo_vigente
         		ELSE rgo.gttcrgo_rgo_imp_cubierto END as DECIMAL(19,4))   					        			fc_gar_contrato_valoractualizado,
CAST(rgo.gttcrgo_rgo_por_cubierto  as DECIMAL(19,4))                              								ds_gar_contrato_porcentajecobertura,

rgb.gttcrgb_rgb_num_bien																						cod_gar_num_bien,
bie.gttcbie_bie_cod_bien 																						cod_gar_bien,
bie.gttcbie_bie_des_bien   																						ds_gar_bien_descripcion,
CASE WHEN rgb.gttcrgb_rgb_fec_inivali='0001-01-01' THEN NULL ELSE rgb.gttcrgb_rgb_fec_inivali END 				dt_gar_fecha_inivali,
CASE WHEN rgb.gttcrgb_rgb_fec_finvali='9999-12-31' THEN NULL ELSE rgb.gttcrgb_rgb_fec_finvali END				dt_gar_fecha_finvali,
CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL THEN vab_ori.gttcvab_vab_imp_total
	 ELSE tasa_ori.gttcvab_vab_imp_total END 	 															    fc_gar_bien_valororiginal,
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL THEN vab_ori.gttcvab_vab_cod_divisa
	 ELSE tasa_ori.gttcvab_vab_cod_divisa END 																	cod_gar_bien_divisaoriginal,
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
 THEN CAST(CASE WHEN vab_ori.gttcvab_vab_cod_divisa !='ARS'
 	      THEN vab_ori.gttcvab_vab_imp_total * cotizaciones_rgb.imp_cambio_fijo_vigente
          ELSE vab.gttcvab_vab_imp_total END as DECIMAL(19,4))
 ELSE CAST(CASE WHEN tasa_ori.gttcvab_vab_cod_divisa !='ARS'
 	      THEN tasa_ori.gttcvab_vab_imp_total * cotizaciones_tasa.imp_cambio_fijo_vigente
          ELSE tasa.gttcvab_vab_imp_total END as DECIMAL(19,4)) 					END							fc_gar_bien_valoractualizado,
IF (vab_ori.gttcvab_vab_cod_divisa !='ARS' OR tasa_ori.gttcvab_vab_cod_divisa !='ARS',
	CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
		 THEN cotizaciones_rgb.imp_cambio_fijo_vigente
		 ELSE cotizaciones_tasa.imp_cambio_fijo_vigente END ,1)						    						fc_gar_bien_cambioactualizado,
IF (vab_ori.gttcvab_vab_cod_divisa !='ARS' OR tasa_ori.gttcvab_vab_cod_divisa !='ARS',
   CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
   		THEN cotizaciones_rgb.partition_date
   		ELSE cotizaciones_tasa.partition_date END,
   CASE WHEN vab.gttcvab_vab_cod_divisa IS NOT NULL
   		THEN SUBSTRING(vab.gttcvab_vab_timest_umo,1,10)
   		ELSE SUBSTRING(tasa.gttcvab_vab_timest_umo,1,10) END)													dt_gar_bien_fechaactualizacion,
rgb.gttcrgb_rgb_por_biegaran 																					ds_gar_bien_porcentajecobertura,
hip.gttchip_hip_tip_propieda 																					cod_gar_tipo_propiedadhip,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
  cast(((CASE WHEN tasa_ori.gttcvab_vab_imp_total IS NOT NULL THEN tasa_ori.gttcvab_vab_imp_total
	 ELSE  vab_ori.gttcvab_vab_imp_total END) /uva_ori.uva_ori) as DECIMAL(19,4)), NULL)						fc_gar_uva_importeoriginal,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null ,uva_ori.uva_ori ,null)				       						fc_gar_uva_valororigen,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
   cast(((CASE WHEN tasa_ori.gttcvab_vab_imp_total IS NOT NULL THEN tasa_ori.gttcvab_vab_imp_total
	 ELSE vab_ori.gttcvab_vab_imp_total END) /uva_ori.uva_ori)*uva.uva as DECIMAL(19,4)), NULL)					fc_gar_uva_valoractualizado,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
SUBSTRING(mae.partition_date,1,10) ,null)				    													dt_gar_uva_fechactualizacion,
CASE WHEN hip_uva.gttcvab_vab_imp_total IS NOT NULL or prod_uva.codigo_producto is not null
	THEN CAST(((tasa.gttcvab_vab_imp_total /uva_ori.uva_ori)* uva.uva) AS DECIMAL(19,4))  ELSE NULL END			fc_gar_uva_tasacionactualizado,
hip_uva.gttcvab_vab_imp_total																					fc_gar_uva_pesos,
tasa.gttcvab_vab_cod_divisa							    														cod_gar_divisa_tasacion,
CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4))															fc_gar_importe_tasacionoriginal,
CAST(tasa.gttcvab_vab_imp_total as DECIMAL(19,4))																fc_gar_importe_tasacion,
CASE WHEN gttcgar_gar_cla_garantia ='001' THEN vab.gttcvab_vab_cod_divisa		ELSE NULL END					cod_gar_divisa_escritura,
CASE WHEN gttcgar_gar_cla_garantia='001'
     THEN CAST(vab.gttcvab_vab_imp_total as DECIMAL(19,4))		ELSE NULL END 					                fc_gar_importe_escritura,
CAST(rgo.gttcrgo_rgo_imp_deuda 	as DECIMAL(19,4))															    fc_gar_importe_deuda,


IF(prestamos.imp_deuda_original IS NOT NULL ,
CAST(prestamos.imp_deuda_original as DECIMAL(19,4))/CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL
    THEN CAST(vab_ori.gttcvab_vab_imp_total as DECIMAL(19,4))
    ELSE CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4)) END ,
rgo.gttcrgo_rgo_imp_deuda /CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL
    THEN CAST(vab_ori.gttcvab_vab_imp_total as DECIMAL(19,4))
    ELSE CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4)) END) 											fc_gar_LTV_origen,

IF(hip_uva.gttcvab_vab_num_bien is not null ,
CAST((rgo.gttcrgo_rgo_imp_deuda/
   		((CASE WHEN tasa.gttcvab_vab_imp_total IS NOT NULL
   			THEN tasa.gttcvab_vab_imp_total
	 		ELSE vab.gttcvab_vab_imp_total END)/uva_ori.uva_ori)*uva.uva) AS DECIMAL(19,4)) ,

CAST((rgo.gttcrgo_rgo_imp_deuda/
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
 	 THEN 	CASE   WHEN vab_ori.gttcvab_vab_cod_divisa !='ARS'
 	      		   THEN vab_ori.gttcvab_vab_imp_total * cotizaciones_rgb.imp_cambio_fijo_vigente
          		   ELSE vab.gttcvab_vab_imp_total END

     ELSE   CASE   WHEN tasa_ori.gttcvab_vab_cod_divisa !='ARS'
 	      		   THEN tasa_ori.gttcvab_vab_imp_total * cotizaciones_tasa.imp_cambio_fijo_vigente
                   ELSE tasa.gttcvab_vab_imp_total END 	END	)		AS DECIMAL(19,4)))	                        fc_gar_LTV_actual,

 (1-(rgo.gttcrgo_rgo_imp_deuda/
 				(CASE WHEN mae.gttcmae_mae_cod_divisa !='ARS'
	      			  THEN CAST(mae.gttcmae_mae_imp_limite AS DECIMAL(19,4))*
	      			  CAST(cotizaciones_mae.imp_cambio_fijo_vigente AS DECIMAL(19,4))
        			  ELSE CAST(mae.gttcmae_mae_imp_limite as DECIMAL(19,4)) END)))*100 						fc_gar_garantia_porcentajedisponible,
rtg_empresa.cal_unif_empresa																					fc_gar_calificacion_empresa,
rtg_pais.cal_unif_pais  																						fc_gar_calificacion_pais,
b.provincia																										ds_gar_ubicacion,
mae.partition_date																								partition_date
FROM bi_corp_staging.gtdtmae mae
LEFT JOIN  rgb on (mae.gttcmae_mae_num_garantia= rgb.gttcrgb_rgb_num_garantia )
LEFT JOIN rgb_nup on (mae.gttcmae_mae_num_garantia= rgb_nup.gttcrgb_rgb_num_garantia )
LEFT JOIN bi_corp_staging.gtdtbie bie on (bie.gttcbie_bie_num_bien=rgb.gttcrgb_rgb_num_bien  and mae.partition_date=bie.partition_date)
LEFT JOIN bi_corp_staging.gtdtgar gar on (gar.gttcgar_gar_cod_garantia= mae.gttcmae_mae_cod_garantia  and mae.partition_date=gar.partition_date)
LEFT JOIN
(select *
 from bi_corp_staging.gtdtrgo
 where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
  AND gttcrgo_rgo_fec_altrelac <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
  and gttcrgo_rgo_fec_bajrelac>= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' ) rgo
 on (rgo.gttcrgo_rgo_num_garantia=mae.gttcmae_mae_num_garantia ) -- contratos activos
--LEFT JOIN bi_corp_staging.gtdtrgc rgc  on (rgc.gttcrgc_rgc_num_garantia=mae.gttcmae_mae_num_garantia and mae.partition_date=rgc.partition_date)
LEFT JOIN
 (select gttcrbc_rbc_num_bien,gttcrbc_rbc_num_persona
   from ( select row_number() OVER (partition by gttcrbc_rbc_num_bien, gttcrbc_rbc_num_persona order by gttcrbc_rbc_timest_umo desc) as orden,
           gttcrbc_rbc_num_bien ,
   	 	  gttcrbc_rbc_num_persona
    	  FROM bi_corp_staging.gtdtrbc
        where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' ) rg
        where orden=1 ) rbc
        on rbc.gttcrbc_rbc_num_bien =rgb_nup.gttcrgb_rgb_num_bien

LEFT JOIN bi_corp_staging.gtdthip hip on (hip.gttchip_hip_num_bien=rgb.gttcrgb_rgb_num_bien and mae.partition_date=hip.partition_date)
LEFT JOIN
	(SELECT dom.gen_clave pecodpro, trim(substr(dom.gen_datos, 1, 30)) provincia
	FROM bi_corp_staging.tcdtgen dom WHERE dom.gentabla = '0009' AND partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}') b
	ON trim(b.pecodpro) = SUBSTRING(trim(gttchip_hip_cod_provinci),2)
LEFT JOIN imp_lim_garantia ON imp_lim_garantia.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN vab ON vab.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN prod_uva on (rgo.gttcrgo_rgo_contrato_rgo_cod_producto= prod_uva.codigo_producto
 and rgo.gttcrgo_rgo_contrato_rgo_cod_subprod=prod_uva.codigo_subproducto)
LEFT JOIN vab_ori ON vab_ori.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN hip_uva  ON hip_uva.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN hip_uva_ori ON (hip_uva_ori.bien= rgb.gttcrgb_rgb_num_bien and rgb.gttcrgb_rgb_num_garantia=hip_uva_ori.garantia)
LEFT JOIN tasa ON tasa.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN tasa_ori ON tasa_ori.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN val_nominal ON val_nominal.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN cotizaciones AS cotizaciones_rgb ON (cotizaciones_rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_rgb.cod_divisa = vab_ori.gttcvab_vab_cod_divisa)
LEFT JOIN cotizaciones AS cotizaciones_rgo ON (cotizaciones_rgo.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_rgo.cod_divisa = rgo.gttcrgo_rgo_contrato_rgo_cod_divcont)
LEFT JOIN cotizaciones AS cotizaciones_mae ON (cotizaciones_mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_mae.cod_divisa = mae.gttcmae_mae_cod_divisa)
LEFT JOIN cotizaciones AS cotizaciones_tasa ON  (cotizaciones_tasa.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_tasa.cod_divisa = tasa_ori.gttcvab_vab_cod_divisa)
LEFT JOIN uva AS uva ON uva.fecha= mae.partition_date
LEFT JOIN prestamos ON (rgo.gttcrgo_rgo_contrato_rgo_cod_oficont=prestamos.oficina and rgo.gttcrgo_rgo_contrato_rgo_num_ctacont = prestamos.cuenta)
LEFT JOIN uva_ori ON uva_ori.fecha= prestamos.fecha_alta_contrato
LEFT JOIN rtg_pais 	ON ( rbc.gttcrbc_rbc_num_persona = rtg_pais.penumper)
LEFT JOIN bi_corp_bdr.aux_garant_calif_empresa rtg_empresa ON (rbc.gttcrbc_rbc_num_persona = rtg_empresa.alias_nup and rtg_empresa.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_GARANTIAS') }}')
LEFT JOIN VAB_ORI_GAR gar_ori ON mae.gttcmae_mae_num_garantia= gar_ori.garantia

where mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'  and mae.gttcmae_mae_tip_cobertur = '001'

UNION ALL


-- GARANTIAS GENERICAS----

select
distinct mae.gttcmae_mae_cod_entidad 																		 cod_gar_entidad,
mae.gttcmae_mae_num_garantia 						       													 cod_gar_num_garantia,
mae.gttcmae_mae_cod_garantia    																			 cod_gar_garantia,
gar.gttcgar_gar_des_corta  																					 ds_gar_garantia,
gar.gttcgar_gar_cla_garantia  																				 cod_gar_cla_garantia,
gar.gttcgar_gar_des_larga																					 ds_gar_cla_garantia,
mae.gttcmae_mae_tip_cobertur    																			 cod_gar_tipo_cobertura,
'GENÉRICA'																									 ds_gar_tipo_cobertura,
CASE WHEN CAST(rgc.gttcrgc_rgc_num_persona AS BIGINT) IS NOT NULL
     THEN CAST(rgc.gttcrgc_rgc_num_persona AS BIGINT)
     ELSE CAST(rbc.gttcrbc_rbc_num_persona AS BIGINT) END													 cod_per_nup,
rgo.gttcrgo_rgo_tip_relacion	 							                                                 cod_gar_relacion,
CAST(rgo.gttcrgo_rgo_contrato_rgo_cod_oficont AS INT)								                         cod_gar_sucural,
CAST(rgo.gttcrgo_rgo_contrato_rgo_num_ctacont AS BIGINT)													 cod_gar_num_cuenta,
rgo.gttcrgo_rgo_contrato_rgo_cod_producto																	 cod_gar_producto,
rgo.gttcrgo_rgo_contrato_rgo_cod_subprod																	 cod_gar_subproducto,
CASE WHEN mae.gttcmae_mae_fec_alta='0001-01-01' THEN NULL ELSE mae.gttcmae_mae_fec_alta END 				 dt_gar_fecha_alta,
CASE WHEN mae.gttcmae_mae_fec_vcto='9999-12-31' THEN NULL ELSE mae.gttcmae_mae_fec_vcto END					 dt_gar_fecha_vencimiento,
mae.gttcmae_mae_fec_activa																					 dt_gar_fecha_activacion,
mae.gttcmae_mae_cod_divisa																					 cod_gar_divisa,
IF (gar_ori.garantia IS NOT NULL,
    gar_ori.valor_ori,gttcmae_mae_imp_limite)																 fc_gar_importe_limiteoriginal,
CASE WHEN mae.gttcmae_mae_cod_divisa !='ARS'
	      		THEN CAST(mae.gttcmae_mae_imp_limite AS DECIMAL(19,4))*
	      		 CAST(cotizaciones_mae.imp_cambio_fijo_vigente AS DECIMAL(19,4))
         		ELSE CAST( mae.gttcmae_mae_imp_limite as DECIMAL(19,4)) END									 fc_gar_importe_limiteactualizado,
mae.gttcmae_mae_fec_camlim																					 dt_gar_fecha_cambiolimite,
CASE WHEN mae.gttcmae_mae_ind_bancosor= 'S' THEN 1 ELSE 0 END 											     flag_gar_bancosor,
mae.gttcmae_mae_cod_estado																					 cod_gar_estado,
CASE WHEN mae.gttcmae_mae_cod_estado='010' THEN 'TRAMITE'
     WHEN mae.gttcmae_mae_cod_estado='020' THEN 'ACTIVO'
     WHEN mae.gttcmae_mae_cod_estado='030' THEN 'NO ACTIVO'
     ELSE NULL END                                                                                           ds_gar_estado,
mae.gttcmae_mae_cod_subestad																				 cod_gar_subestado,
CAST(mae.gttcmae_mae_imp_disponib AS DECIMAL(19,4))															 fc_gar_importe_disponible,
CAST(mae.gttcmae_mae_imp_alzado	AS DECIMAL(19,4))															 fc_gar_importe_alzado,
CAST(mae.gttcmae_mae_imp_contable	AS DECIMAL(19,4))														 fc_gar_importe_contable,
mae.gttcmae_mae_fec_ultcober																				 dt_gar_fecha_ultimacobertura,
CASE WHEN mae.gttcmae_mae_ind_bancosor= 'S' THEN 1 ELSE 0 END												 flag_gar_reconstitucion,
rgo.gttcrgo_rgo_imp_cubierto																	 			 fc_gar_contrato_valororiginal,
rgo.gttcrgo_rgo_contrato_rgo_cod_divcont                                                					 cod_gar_contrato_divisaoriginal,
CAST(CASE WHEN rgo.gttcrgo_rgo_contrato_rgo_cod_divcont !='ARS'
	      		THEN rgo.gttcrgo_rgo_imp_cubierto * cotizaciones_rgo.imp_cambio_fijo_vigente
         		ELSE rgo.gttcrgo_rgo_imp_cubierto END as DECIMAL(19,4))   					        		 fc_gar_contrato_valoractualizado,
CAST(rgo.gttcrgo_rgo_por_cubierto  as DECIMAL(19,4))                              							 ds_gar_contrato_porcentajecobertura,
rgb.gttcrgb_rgb_num_bien																					 cod_gar_num_bien,
bie.gttcbie_bie_cod_bien 																					 cod_gar_bien,
bie.gttcbie_bie_des_bien   																					 ds_gar_bien_descripcion,
CASE WHEN rgb.gttcrgb_rgb_fec_inivali='0001-01-01' THEN NULL ELSE rgb.gttcrgb_rgb_fec_inivali END 			 dt_gar_fecha_inivali,
CASE WHEN rgb.gttcrgb_rgb_fec_finvali='9999-12-31' THEN NULL ELSE rgb.gttcrgb_rgb_fec_finvali END		     dt_gar_fecha_finvali,
CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL THEN vab_ori.gttcvab_vab_imp_total
	 ELSE tasa_ori.gttcvab_vab_imp_total END 																 fc_gar_bien_valororiginal,
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL THEN vab_ori.gttcvab_vab_cod_divisa
	 ELSE tasa_ori.gttcvab_vab_cod_divisa END 																 cod_gar_bien_divisaoriginal,
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
 THEN CAST(CASE WHEN vab_ori.gttcvab_vab_cod_divisa !='ARS'
 	      THEN vab_ori.gttcvab_vab_imp_total * cotizaciones_rgb.imp_cambio_fijo_vigente
          ELSE vab.gttcvab_vab_imp_total END as DECIMAL(19,4))
 ELSE CAST(CASE WHEN tasa_ori.gttcvab_vab_cod_divisa !='ARS'
 	      THEN tasa_ori.gttcvab_vab_imp_total * cotizaciones_tasa.imp_cambio_fijo_vigente
          ELSE tasa.gttcvab_vab_imp_total END as DECIMAL(19,4)) 					END						 fc_gar_bien_valoractualizado,
IF (vab_ori.gttcvab_vab_cod_divisa !='ARS' OR tasa_ori.gttcvab_vab_cod_divisa !='ARS',
	CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
		 THEN cotizaciones_rgb.imp_cambio_fijo_vigente
		 ELSE cotizaciones_tasa.imp_cambio_fijo_vigente END ,1)						                         fc_gar_bien_cambioactualizado,
IF (vab_ori.gttcvab_vab_cod_divisa !='ARS' OR tasa_ori.gttcvab_vab_cod_divisa !='ARS',
   CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
   		THEN cotizaciones_rgb.partition_date
   		ELSE cotizaciones_tasa.partition_date END,
   CASE WHEN vab.gttcvab_vab_cod_divisa IS NOT NULL
   		THEN SUBSTRING(vab.gttcvab_vab_timest_umo,1,10)
   		ELSE SUBSTRING(tasa.gttcvab_vab_timest_umo,1,10) END)                             					 dt_gar_bien_fechaactualizacion,
cast(rgb.gttcrgb_rgb_por_biegaran as DECIMAL(19,4))															 ds_gar_bien_porcentajecobertura,
hip.gttchip_hip_tip_propieda 																				 cod_gar_tipo_propiedadhip,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
  cast(((CASE WHEN tasa_ori.gttcvab_vab_imp_total IS NOT NULL THEN tasa_ori.gttcvab_vab_imp_total
	 ELSE  vab_ori.gttcvab_vab_imp_total END) /uva_ori.uva_ori) as DECIMAL(19,4)), NULL)						fc_gar_uva_importeoriginal,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null ,uva_ori.uva_ori ,null)				       						fc_gar_uva_valororigen,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
   cast(((CASE WHEN tasa_ori.gttcvab_vab_imp_total IS NOT NULL THEN tasa_ori.gttcvab_vab_imp_total
	 ELSE vab_ori.gttcvab_vab_imp_total END) /uva_ori.uva_ori)*uva.uva as DECIMAL(19,4)), NULL)					fc_gar_uva_valoractualizado,
IF (hip_uva.gttcvab_vab_num_bien  is not null or prod_uva.codigo_producto is not null,
SUBSTRING(mae.partition_date,1,10) ,null)				    													dt_gar_uva_fechactualizacion,
CASE WHEN hip_uva.gttcvab_vab_imp_total IS NOT NULL or prod_uva.codigo_producto is not null
	THEN CAST(((tasa.gttcvab_vab_imp_total /uva_ori.uva_ori)* uva.uva) AS DECIMAL(19,4))  ELSE NULL END			fc_gar_uva_tasacionactualizado,
hip_uva.gttcvab_vab_imp_total																					fc_gar_uva_pesos,
tasa.gttcvab_vab_cod_divisa							    														cod_gar_divisa_tasacion,
CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4))															fc_gar_importe_tasacionoriginal,
CAST(tasa.gttcvab_vab_imp_total as DECIMAL(19,4))																fc_gar_importe_tasacion,
CASE WHEN gttcgar_gar_cla_garantia ='001' THEN vab.gttcvab_vab_cod_divisa		ELSE NULL END					cod_gar_divisa_escritura,
CASE WHEN gttcgar_gar_cla_garantia='001'
     THEN CAST(vab.gttcvab_vab_imp_total as DECIMAL(19,4))		ELSE NULL END 					                fc_gar_importe_escritura,
CAST(rgo.gttcrgo_rgo_imp_deuda 	as DECIMAL(19,4))															    fc_gar_importe_deuda,


IF(prestamos.imp_deuda_original IS NOT NULL ,
CAST(prestamos.imp_deuda_original as DECIMAL(19,4))/CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL
    THEN CAST(vab_ori.gttcvab_vab_imp_total as DECIMAL(19,4))
    ELSE CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4)) END ,
rgo.gttcrgo_rgo_imp_deuda /CASE WHEN vab_ori.gttcvab_vab_imp_total IS NOT NULL
    THEN CAST(vab_ori.gttcvab_vab_imp_total as DECIMAL(19,4))
    ELSE CAST(tasa_ori.gttcvab_vab_imp_total as DECIMAL(19,4)) END) 											fc_gar_LTV_origen,

IF(hip_uva.gttcvab_vab_num_bien is not null ,
CAST((rgo.gttcrgo_rgo_imp_deuda/
   		((CASE WHEN tasa.gttcvab_vab_imp_total IS NOT NULL
   			THEN tasa.gttcvab_vab_imp_total
	 		ELSE vab.gttcvab_vab_imp_total END)/uva_ori.uva_ori)*uva.uva) AS DECIMAL(19,4)) ,

CAST((rgo.gttcrgo_rgo_imp_deuda/
CASE WHEN vab_ori.gttcvab_vab_cod_divisa IS NOT NULL
 	 THEN 	CASE   WHEN vab_ori.gttcvab_vab_cod_divisa !='ARS'
 	      		   THEN vab_ori.gttcvab_vab_imp_total * cotizaciones_rgb.imp_cambio_fijo_vigente
          		   ELSE vab.gttcvab_vab_imp_total END

     ELSE   CASE   WHEN tasa_ori.gttcvab_vab_cod_divisa !='ARS'
 	      		   THEN tasa_ori.gttcvab_vab_imp_total * cotizaciones_tasa.imp_cambio_fijo_vigente
                   ELSE tasa.gttcvab_vab_imp_total END 	END	)		AS DECIMAL(19,4)))	                        fc_gar_LTV_actual,

 (1-(rgo.gttcrgo_rgo_imp_deuda/
 				(CASE WHEN mae.gttcmae_mae_cod_divisa !='ARS'
	      			  THEN CAST(mae.gttcmae_mae_imp_limite AS DECIMAL(19,4))*
	      			  CAST(cotizaciones_mae.imp_cambio_fijo_vigente AS DECIMAL(19,4))
        			  ELSE CAST(mae.gttcmae_mae_imp_limite as DECIMAL(19,4)) END)))*100 					 fc_gar_garantia_porcentajedisponible,
rtg_empresa.cal_unif_empresa																				 fc_gar_calificacion_empresa,
rtg_pais.cal_unif_pais  																					 fc_gar_calificacion_pais,
b.provincia																									 ds_gar_ubicacion,
mae.partition_date										    												 partition_date


FROM bi_corp_staging.gtdtmae mae
LEFT JOIN  rgb on (mae.gttcmae_mae_num_garantia= rgb.gttcrgb_rgb_num_garantia )
LEFT JOIN bi_corp_staging.gtdtbie bie on (bie.gttcbie_bie_num_bien=rgb.gttcrgb_rgb_num_bien  and mae.partition_date=bie.partition_date)
LEFT JOIN bi_corp_staging.gtdtgar gar on (gar.gttcgar_gar_cod_garantia= mae.gttcmae_mae_cod_garantia  and mae.partition_date=gar.partition_date)
LEFT JOIN
(select *
 from bi_corp_staging.gtdtrgo
 where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
 AND gttcrgo_rgo_fec_altrelac <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
 and gttcrgo_rgo_fec_bajrelac>= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' ) rgo
 on (rgo.gttcrgo_rgo_num_garantia=mae.gttcmae_mae_num_garantia) -- contratos activos
LEFT JOIN
 (select gttcrbc_rbc_num_bien,gttcrbc_rbc_num_persona
   from ( select row_number() OVER (partition by gttcrbc_rbc_num_bien order by gttcrbc_rbc_timest_umo desc) as orden,
           gttcrbc_rbc_num_bien ,
   	 	  gttcrbc_rbc_num_persona
    	  FROM bi_corp_staging.gtdtrbc
        where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' ) rg
        where orden=1 ) rbc
        on rbc.gttcrbc_rbc_num_bien =rgb.gttcrgb_rgb_num_bien -- por si no hay nup en rgc
LEFT JOIN
(select gttcrgc_rgc_num_garantia,  gttcrgc_rgc_num_persona
	from (select row_number() OVER (partition by gttcrgc_rgc_num_garantia, gttcrgc_rgc_num_persona order by gttcrgc_rgc_timest_umo desc) as orden,
          gttcrgc_rgc_num_garantia ,
  	 	  gttcrgc_rgc_num_persona
   	  FROM bi_corp_staging.gtdtrgc
       where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}') rg
       where orden=1 ) rgc
      on rgc.gttcrgc_rgc_num_garantia=mae.gttcmae_mae_num_garantia
LEFT JOIN bi_corp_staging.gtdthip hip on (hip.gttchip_hip_num_bien=rgb.gttcrgb_rgb_num_bien and mae.partition_date=hip.partition_date)
LEFT OUTER JOIN
	(SELECT dom.gen_clave pecodpro, trim(substr(dom.gen_datos, 1, 30)) provincia
	FROM bi_corp_staging.tcdtgen dom WHERE dom.gentabla = '0009' AND partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}') b
	ON trim(b.pecodpro) = SUBSTRING(trim(gttchip_hip_cod_provinci),2)
LEFT JOIN imp_lim_garantia ON imp_lim_garantia.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN vab ON vab.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN prod_uva on (rgo.gttcrgo_rgo_contrato_rgo_cod_producto= prod_uva.codigo_producto
 and rgo.gttcrgo_rgo_contrato_rgo_cod_subprod=prod_uva.codigo_subproducto)
LEFT JOIN vab_ori ON vab_ori.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN hip_uva  ON hip_uva.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN hip_uva_ori ON (hip_uva_ori.bien= rgb.gttcrgb_rgb_num_bien and rgb.gttcrgb_rgb_num_garantia=hip_uva_ori.garantia)
LEFT JOIN tasa ON tasa.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN tasa_ori ON tasa_ori.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN val_nominal ON val_nominal.gttcvab_vab_num_bien= rgb.gttcrgb_rgb_num_bien
LEFT JOIN cotizaciones AS cotizaciones_rgb ON (cotizaciones_rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_rgb.cod_divisa = vab_ori.gttcvab_vab_cod_divisa)
LEFT JOIN cotizaciones AS cotizaciones_rgo ON (cotizaciones_rgo.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_rgo.cod_divisa = rgo.gttcrgo_rgo_contrato_rgo_cod_divcont)
LEFT JOIN cotizaciones AS cotizaciones_mae ON (cotizaciones_mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'and cotizaciones_mae.cod_divisa = mae.gttcmae_mae_cod_divisa)
LEFT JOIN cotizaciones AS cotizaciones_tasa ON ( cotizaciones_tasa.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' and cotizaciones_tasa.cod_divisa = tasa_ori.gttcvab_vab_cod_divisa)
LEFT JOIN uva AS uva ON uva.fecha= mae.partition_date
LEFT JOIN prestamos ON (rgo.gttcrgo_rgo_contrato_rgo_cod_oficont=prestamos.oficina and rgo.gttcrgo_rgo_contrato_rgo_num_ctacont = prestamos.cuenta)
LEFT JOIN uva_ori ON uva_ori.fecha= prestamos.fecha_alta_contrato
LEFT JOIN rtg_pais 	ON ( rbc.gttcrbc_rbc_num_persona = rtg_pais.penumper)
LEFT JOIN bi_corp_bdr.aux_garant_calif_empresa rtg_empresa ON (rbc.gttcrbc_rbc_num_persona = rtg_empresa.alias_nup and rtg_empresa.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_GARANTIAS') }}')
LEFT JOIN VAB_ORI_GAR gar_ori ON mae.gttcmae_mae_num_garantia= gar_ori.garantia

where mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'  and mae.gttcmae_mae_tip_cobertur = '002'


UNION ALL ----------------------- todos los datos provenientes de ABKT
 SELECT
 		 CONTRAG.cod_entidad															cod_gar_entidad,
 		 CONTRAG.num_garantia															cod_gar_num_garantia,
 		 CONTRAG.cod_garantia															cod_gar_garantia,
 		 'FIANZA' 																		ds_gar_garantia,
 		 CONTRAG.cla_garantia															cod_gar_cla_garantia,
 		 'FZAS BCOS DEL EXTERIOR CALIFICACION A EN MN'									ds_gar_cla_garantia,
 		 CONTRAG.tip_cobertur															cod_gar_tipo_cobertura,
 		 'ESPECÍFICA'																	ds_gar_tipo_cobertura,
 		 cast(CONTRAG.cgr_nup_beneficiario as bigint)									cod_per_nup,
 		 NULL																			cod_gar_relacion,
 		 NULL																			cod_gar_sucural,
 		 cast(CONTRAG.id_cto as bigint)													cod_gar_num_cuenta,
 	 	 adsf.isec_pro_producto															cod_gar_producto,
 	 	 adsf.isec_spr_subproducto														cod_gar_subproducto,
 		 CONTRAG.fec_alta																dt_gar_fecha_alta,
 		 CONTRAG.fec_vcto																dt_gar_fecha_vencimiento,
 	 	 NULL																			dt_gar_fecha_activacion,
 	 	 CASE WHEN CONTRAG.cod_divisa= '$'
 	 	      THEN 'ARS' ELSE CONTRAG.cod_divisa END 									cod_gar_divisa,
 	 	 CONTRAG.imp_limite																fc_gar_importe_limiteoriginal,
 		 CASE WHEN CONTRAG.cod_divisa!='$'
 		 	  THEN CONTRAG.imp_limite*cotizaciones.imp_cambio_fijo_vigente
 		 	  ELSE CONTRAG.imp_limite				end							        fc_gar_importe_limiteactualizado,
 		 CONTRAG.partition_date														    dt_gar_fecha_cambiolimite,
 		 NULL																			flag_gar_bancosor,
 		 CONTRAG.cod_estado																cod_gar_estado,
 		 'ACTIVO'																		ds_gar_estado,
 		 NULL																			cod_gar_subestado,
 	     NULL																			fc_gar_importe_disponible,
 		 NULL                                                                           fc_gar_importe_alzado,
 		 NULL 																		    fc_gar_importe_contable,
 		 NULL                                                                           dt_gar_fecha_ultimacobertura,
          NULL																			flag_gar_reconstitucion,
 		 CONTRAG.imp_limite																fc_gar_contrato_valororiginal,
 	    CASE WHEN CONTRAG.cod_divisa= '$'
 	 	      THEN 'ARS' ELSE CONTRAG.cod_divisa END	 								cod_gar_contrato_divisaoriginal,
          CASE WHEN CONTRAG.cod_divisa!='$'
 		 	  THEN CONTRAG.imp_limite*cotizaciones.imp_cambio_fijo_vigente
 		 	  ELSE CONTRAG.imp_limite 				end									fc_gar_contrato_valoractualizado,
 		 NULL 																			ds_gar_contrato_porcentajecobertura,
 		 NULL                                                                           cod_gar_num_bien,
 		 NULL                                                                           cod_gar_bien,
          NULL																		    ds_gar_bien_descripcion,
          NULL 																			dt_gar_fecha_inivali,
          NULL                                                                           dt_gar_fecha_finvali,
 		 NULL                                                                           fc_gar_bien_valororiginal,
 		 NULL                                                                           cod_gar_bien_divisaoriginal,
          NULL																			fc_gar_bien_valoractualizado,
          NULL																			fc_gar_bien_cambioactualizado,
 		 NULL 																			dt_gar_bien_fechaactualizacion,
          NULL																			ds_gar_bien_porcentajecobertura,
          NULL																			cod_gar_tipo_propiedadhip,
          NULL																			fc_gar_uva_importeoriginal,
          NULL																			fc_gar_uva_valororigen,
          NULL 																			fc_gar_uva_valoractualizado,
          NULL 																			dt_gar_uva_fechactualizacion,
 	     NULL																			fc_gar_uva_tasacionactualizado,
 	     NULL																		    fc_gar_uva_pesos,
          NULL 																			cod_gar_divisa_tasacion,
          NULL																			fc_gar_importe_tasacionoriginal,
          NULL 																			fc_gar_importe_tasacion,
          NULL 																			cod_gar_divisa_escritura,
          NULL 																			fc_gar_importe_escritura,
 		 adsf.isec_imp_deuda 															fc_gar_importe_deuda,
          NULL 																			fc_gar_LTV_origen,
          NULL 																			fc_gar_LTV_actual,
          (1-(adsf.isec_imp_deuda/CASE WHEN CONTRAG.cod_divisa!='$'
 		 	  THEN CONTRAG.imp_limite*cotizaciones.imp_cambio_fijo_vigente
 		 	  ELSE CONTRAG.imp_limite end ))*100  										fc_gar_garantia_porcentajedisponible,
 		 NULL 																			fc_gar_calificacion_empresa,
 		 NULL 																			fc_gar_calificacion_pais,
 		 NULL                                                                           ds_gar_ubicacion,
 		 CONTRAG.partition_date															partition_date
FROM
		(	SELECT
				  cgr.partition_date
				, '0072' as cod_entidad
				, cgr.cgr_id_contrato  as id_cto
				, cgr.cgr_garantia_nro as num_garantia
				, '020' as cod_estado
				, cgr.cgr_fecha_emision as fec_alta
				, cgr.cgr_fecha_vto as fec_vcto
				, SUBSTRING(TRIM(cgr.cgr_moneda),1,3) as cod_divisa
				, cgr.cgr_saldo_pesos as imp_limite_pesos
				, cgr.cgr_importe_original imp_limite
				, '009' as cod_garantia
				, '001' as tip_cobertur
				, '003' as cla_garantia
				, cgr.cgr_nup_beneficiario
			FROM 	bi_corp_staging.abkt_contragarantias cgr
			WHERE (cgr.partition_date = IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' < '2020-05-20','2020-05-20' ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' ) )
	 	)  CONTRAG
	 	LEFT JOIN  bi_corp_staging.contratos_adsf_cf adsf on (adsf.isec_cta_cuenta=CONTRAG.id_cto
        AND  adsf.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='first_working_day', dag_id='LOAD_CMN_GARANTIAS') }}' )
        LEFT JOIN cotizaciones  ON (cotizaciones.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'  and cotizaciones.cod_divisa = CONTRAG.cod_divisa)

"