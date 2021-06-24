set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_pyme_estadosoperacion
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select
    s.cod_tramite as cod_adm_tramite,
    s.cod_estado as cod_adm_estado,
    s.cod_estado_accion as cod_adm_estado_accion,
    s.cod_estado_origen as cod_adm_estado_origen,
    s.des_estado as ds_adm_des_estado,
    s.des_sge as ds_adm_sge,
    s.des_srs as ds_adm_srs,
    s.des_datos_adic as ds_adm_datos_adic,
    s.mar_contador as flag_adm_mar_contador,
    s.mar_pisa_paq as flag_adm_mar_pisa_paq,
    cast(s.cant_dias as int) as int_adm_cant_dias,
    cast(s.cant_dia as int) as int_adm_cant_dia,

CASE
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('SW','ZG','SP')
		THEN 'ACEPTADO'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'ACEPTADO'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('AA','AO','AP') AND s.cod_estado_origen IN ('CS') -- scorring
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('AA','AO','AP') AND s.cod_estado_origen IN ('GR') -- scorring
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('OB') -- scorring
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('AA','AH') AND s.cod_estado_origen IN ('VD')  -- scorring
		THEN 'ACEPTADA'
     WHEN s.cod_estado_accion IN ('BA') AND s.cod_estado_origen IN ('OB')
		THEN 'BAJA'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('VD') -- scorring
		THEN 'DECLINADA'
	WHEN s.cod_estado_accion IN ('DE','DF') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'DECLINADO'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='D' AND s.cod_estado_origen IN ('SW') --SCORGIN
		THEN 'DECLINADO'

	 WHEN s.cod_estado_accion IN ('DL','DL','DB','DX') AND s.cod_estado_origen IN ('OB') -- scorring
		THEN 'DECLINADO'
	WHEN s.cod_estado_accion IN ('RI') AND s.cod_estado_origen IN ('GR') AND s.des_estado='Rechazo del Oficial Riesgos (Cesion Granel)' -- SUPERVISION
		THEN 'DECLINADO'
	WHEN s.cod_estado_accion IN ('RI') AND s.cod_estado_origen IN ('GR') AND s.des_estado='Aceptado Riesgos - Propuesta Env Cliente (Cesion Granel)' -- SUPERVISION
		THEN 'ACEPTADO'
	WHEN s.cod_estado_accion IN ('DG') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'DEVUELTO'
	WHEN s.cod_estado_accion IN ('SP') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'PENDIENTE SUPERVISION'
	WHEN s.cod_estado_accion IN ('DN') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'VENCIDA'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='D' AND s.cod_estado_origen IN ('CS')
		THEN 'DECLINADO'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('CS') AND s.des_estado<>'Error en impacto de cambio de estado (Cesion Suc)'
		THEN 'DECLINADO'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('GR') --SCORING
		THEN 'DECLINADO'
     WHEN s.cod_estado_accion IN ('OC') AND s.cod_estado_origen IN ('GR')
		THEN 'BAJA GRANEL'
     WHEN s.cod_estado_accion IN ('CL') AND s.cod_estado_origen IN ('GR')
		THEN 'BAJA GRANEL'
     WHEN s.cod_estado_accion IN ('VC','VF') AND s.cod_estado_origen IN ('GR')
		THEN 'DECLINADO VALIDACION'
     WHEN s.cod_estado_accion IN ('VC','VF') AND s.cod_estado_origen IN ('GR')
		THEN 'DECLINADO VALIDACION'
      WHEN s.cod_estado_accion IN ('RV') AND s.cod_estado_origen IN ('GR') --SCORING
		THEN 'ZONA GRIS'
      WHEN s.cod_estado_accion IN ('RV') AND s.cod_estado_origen IN ('SW') -- SCORING
		THEN 'ZONA GRIS'
     WHEN s.cod_estado_accion IN ('DC') AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'DECLINADO'
     WHEN s.cod_estado_accion IN ('DD') AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'DECLINADO'
     WHEN s.cod_estado_origen IN ('AU')
		THEN 'PENDIENTE'
     WHEN s.cod_estado_origen IN ('OP')
		THEN 'OPERACIONES'
     WHEN s.cod_estado_origen IN ('AC')
		THEN 'ALTAS CENTRALIZADAS'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('SS')
		THEN 'DECLINADA SUCURSAL'
     WHEN s.cod_estado_accion IN ('DK') AND s.cod_estado_origen IN ('SS')
		THEN 'DESISTIDA SUCURSAL'
     WHEN s.cod_estado_accion IN ('DO','DQ','DR') AND s.cod_estado_origen IN ('SS')
		THEN 'BAJA SUCURSAL'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('SS')
		THEN 'DESISTIDA SUCURSAL'
     WHEN s.cod_estado_accion IN ('SW') AND s.cod_estado_origen IN ('SS')
		THEN 'PENDIENTE SCORING'
     WHEN s.cod_estado_accion IN ('AG','AH') AND s.cod_estado_origen IN ('SS')
		THEN 'LIQUIDADO SUCURSAL'
     WHEN s.cod_estado_accion IN ('CP') AND s.cod_estado_origen IN ('GR')
		THEN 'ACREDITADO GRANEL'
     WHEN s.cod_estado_accion NOT IN ('DO','DQ','DR','DK','DL','RE','SW','AG','AH') AND s.cod_estado_origen IN ('SS')
		THEN 'PENDIENTE'
     WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('AA','AO') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'ACEPTADO'
     WHEN s.cod_estado_accion IN ('DL','DO') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'DECLINADO'
     WHEN s.cod_estado_accion NOT IN ('AA','AO','DL','DO') AND s.cod_estado_origen IN ('ZX') --
		THEN 'PENDIENTE'
     WHEN s.cod_estado_accion  IN ('DO') AND s.cod_estado_origen IN ('VD') --
		THEN 'BAJA'
     WHEN s.cod_estado_accion NOT IN ('AA','AH','DL') AND s.cod_estado_origen IN ('VD') --
		THEN 'PENDIENTE'
      ELSE 'PENDIENTE'
      END as ds_adm_estado,

CASE
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('SW')
		THEN 'SCORING'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'ZONA GRIS'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='A' AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'SUPERVISION'
     WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('CS') AND s.des_estado='Aceptado SW (Cesion Suc)' -- SCORING-- CAMBIO
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('CS') AND s.des_estado='Liquidado (Cesion Suc)' -- SUCURSAL-- CAMBIO
		THEN 'SUCURSAL'
	WHEN s.cod_estado_accion IN ('AO','AP') AND s.cod_estado_origen IN ('CS') -- SUCURSAL-- CAMBIO
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('AA','AO') AND s.cod_estado_origen IN ('GR') -- scorring -- CAMBIO
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('AP') AND s.cod_estado_origen IN ('GR') -- scorring -- CAMBIO
		THEN 'GRANEL'
     WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('OB') -- scorring
		THEN 'ONLINEBANKING'
     WHEN s.cod_estado_accion IN ('AA','AH') AND s.cod_estado_origen IN ('VD')  -- scorring -- CAMBIO
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('BA') AND s.cod_estado_origen IN ('OB')-- CAMBIO
		THEN 'ONLINEBANKING'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('GR') -- scorring
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('VD') -- scorring
		THEN 'SCORING'
	WHEN s.cod_estado_accion IN ('DE','DF') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'SUPERVISION'
     WHEN s.cod_estado_accion IN ('DL','DB','DX') AND s.cod_estado_origen IN ('OB') -- scorring
		THEN 'SCORING'
	WHEN s.cod_estado_accion IN ('RI') AND s.cod_estado_origen IN ('GR') AND s.des_estado='Rechazo del Oficial Riesgos (Cesion Granel)' -- SUPERVISION
		THEN 'SUPERVISION'
	WHEN s.cod_estado_accion IN ('RI') AND s.cod_estado_origen IN ('GR') AND s.des_estado='Aceptado Riesgos - Propuesta Env Cliente (Cesion Granel)' -- SUPERVISION
		THEN 'SUPERVISION'
	WHEN s.cod_estado_accion IN ('DG') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'SUPERVISION'
	WHEN s.cod_estado_accion IN ('SP') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'SUPERVISION'
	WHEN s.cod_estado_accion IN ('DN') AND s.cod_estado_origen IN ('SP') --SUPERVISION
		THEN 'SUPERVISION'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='D' AND s.cod_estado_origen IN ('SW')
		THEN 'SCORING'
	WHEN SUBSTRING(s.cod_estado_accion,1,2)='DL' AND s.cod_estado_origen IN ('CS') AND s.des_estado='Declinado SW final (Cesion Suc)' -- SCORING --CAMBIO
		THEN 'SCORING'
	WHEN SUBSTRING(s.cod_estado_accion,1,1)='D' AND s.cod_estado_origen IN ('CS') AND s.des_estado<>'Declinado SW final (Cesion Suc)' -- CAMBIO
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('CS') AND s.des_estado<>'Error en impacto de cambio de estado (Cesion Suc)' AND s.des_estado<>'Rechazado Riesgos (Cesion Suc)' -- cambio
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('CS') AND s.des_estado='Rechazado Riesgos (Cesion Suc)' -- cambio
		THEN 'SUPERVISION'
     WHEN s.cod_estado_accion IN ('OC') AND s.cod_estado_origen IN ('GR')
		THEN 'GRANEL'
     WHEN s.cod_estado_accion IN ('CL') AND s.cod_estado_origen IN ('GR')
		THEN 'BAJA GRANEL'
     WHEN s.cod_estado_accion IN ('VC','VF') AND s.cod_estado_origen IN ('GR')
		THEN 'GRANEL'
     WHEN s.cod_estado_accion NOT IN ('AA','AO','AP','VC','VF','CL','OC','RI','OC','CL','RV','DL') AND s.cod_estado_origen IN ('GR')
		THEN 'GRANEL'
     WHEN s.cod_estado_accion NOT IN ('AA','BA','DL') AND s.cod_estado_origen IN ('OB')
		THEN 'OBE'
      WHEN s.cod_estado_accion IN ('RV') AND s.cod_estado_origen IN ('GR') --SCORING
		THEN 'SCORING'
      WHEN s.cod_estado_accion IN ('RV') AND s.cod_estado_origen IN ('SW') -- SCORING
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('DC') AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'ZONA GRIS'
     WHEN s.cod_estado_accion IN ('DD') AND s.cod_estado_origen IN ('ZG') --ZONA GRIS
		THEN 'ZONA GRIS'
     WHEN COD_ESTADO_ORIGEN IN ('IN')
		THEN 'SISTEMAS'
     WHEN COD_ESTADO_ORIGEN IN ('JO','GS')
		THEN 'SUCURSAL'
     WHEN COD_ESTADO_ORIGEN IN ('AU')
		THEN 'SISTEMAS'
     WHEN COD_ESTADO_ORIGEN IN ('VI')
		THEN 'SISTEMAS'
     WHEN COD_ESTADO_ORIGEN IN ('OP','PO')
		THEN 'OPERACIONES'
     WHEN COD_ESTADO_ORIGEN IN ('AC')
		THEN 'ALTAS CENTRALIZADAS'
     WHEN s.cod_estado_accion IN ('DL') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('DK') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('DO','DQ','DR') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('SW') AND s.cod_estado_origen IN ('SS')
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('AG','AH') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion NOT IN ('DO','DQ','DR','DK','DL','RE','SW','AG','AH') AND s.cod_estado_origen IN ('SS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion NOT IN ('AA','AO','AP','DO','DQ','DR','DK','DL','RE') AND s.cod_estado_origen IN ('CS')
		THEN 'SUCURSAL'
     WHEN s.cod_estado_accion IN ('RE') AND s.cod_estado_origen IN ('CS') AND s.des_estado='Error en impacto de cambio de estado (Cesion Suc)'
		THEN 'SUCURSAL'
	 WHEN s.cod_estado_accion IN ('AA') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('AA','AO') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'SCORING'
     WHEN s.cod_estado_accion IN ('DL','DO') AND s.cod_estado_origen IN ('ZX') -- SCORING
		THEN 'SCORING'
     WHEN s.cod_estado_accion NOT IN ('AA','AO','DL','DO') AND s.cod_estado_origen IN ('ZX') --
		THEN 'SIN CESION'
     WHEN s.cod_estado_accion  IN ('DO') AND s.cod_estado_origen IN ('VD') --
		THEN 'VENTA TELEFONICA'
     WHEN s.cod_estado_accion NOT IN ('AA','AH','DL') AND s.cod_estado_origen IN ('VD') --
		THEN 'VENTA TELEFONICA'
      ELSE 'PENDIENTE'
      END as ds_adm_sector
from
    bi_corp_staging.sge_stnd_cod_estado s
where
    s.cod_tramite ='F487' and
    concat(s.cod_tramite, s.cod_estado, s.cod_estado_accion, s.cod_estado_origen) not in ("F487PCABSW", "F487PCADZG", "F487PCAFSP", "F487PCDXSW", "F487PCDZSS", "F487RREVAU", "F487RRPOAU", "F487PAPOOP", "F487RRESS", "F487PLSJSS", "F487PASJSS", "F487RROMIN") and
    partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';