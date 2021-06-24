SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table bi_corp_common.stk_pla_plazofijo
partition(partition_date)
select
a.ipf_ipf_entidad  cod_pla_entidad ,
cast(a.ipf_ipf_centro_alta as bigint) cod_pla_sucursal ,
trim(suc.desc_comp_centro_op)   ds_pla_sucursal ,
cast(a.ipf_ipf_cuenta as bigint )  cod_pla_cuenta ,
trim(a.ipf_ipf_producto)    cod_pla_producto ,
trim(a.ipf_ipf_subprodu)    cod_pla_subproducto ,
trim(tcdt039.descripcion)  ds_pla_producto ,
trim(a.ipf_ipf_divisa)    cod_pla_divisa,
a.ipf_ipf_secuencia    cod_pla_secuencia ,
cast(per.wabgpper_nup as bigint) cod_per_nup ,
a.ipf_ipf_secuencia_ren    cod_pla_secrenovacion ,
a.ipf_ipf_plazo    fc_pla_plazo ,
trim(a.ipf_ipf_ind_estado)    cod_pla_estado ,
case when a.ipf_ipf_ind_estado = 'A' then 'ACTIVO'
     when a.ipf_ipf_ind_estado = 'P' then 'PREAPERTURA'
     when a.ipf_ipf_ind_estado = 'V' then 'VENCIDO'
     when a.ipf_ipf_ind_estado = 'C' then 'CANECELADO'
     when a.ipf_ipf_ind_estado = 'Z' then 'ANULADO'
     when a.ipf_ipf_ind_estado = 'X' then 'VENCIDO PENDIENTE PAGO'
     when a.ipf_ipf_ind_estado = 'N' then 'RETENIDO'
     when a.ipf_ipf_ind_estado = 'R' then 'RENUMERADO' ELSE NULL END ds_pla_estado,
a.ipf_ipf_saldo_inicial    fc_pla_saldoinicial ,
(a.ipf_ipf_tipoint + a.ipf_ipf_inc_porc_vig)  fc_pla_tasa ,
a.ipf_ipf_canal_apertura    cod_pla_canalapertura ,
trim(tcdt0222.descripcion)    ds_pla_canalapertura ,
a.ipf_ipf_importe_cta    fc_pla_impcta ,
a.ipf_ipf_importe_sin_cta    fc_pla_impsincta ,
a.ipf_ipf_inteabo    fc_pla_inteabo ,
a.ipf_ipf_int_period     fc_pla_intperiodo ,
a.ipf_ipf_fecha_alta    dt_pla_fechaalta ,
a.ipf_ipf_fecha_opera    dt_pla_fechaopera ,
a.ipf_ipf_fecha_proven   dt_pla_fechavencimiento ,
trim(a.ipf_ipf_tarifa_vig)    cod_pla_tarifavig ,
trim(a.ipf_ipf_ind_custodia)     cod_pla_indcustodia,
a.partition_date partition_date

FROM bi_corp_staging.malbgp_bgtcipf a
LEFT JOIN bi_corp_staging.tcdt050 suc ON suc.cod_centro = lpad(a.ipf_ipf_centro_alta, 5, '0')
LEFT JOIN
(SELECT p.wabgpper_entidad wabgpper_entidad , p.wabgpper_centro_alta wabgpper_centro_alta, p.wabgpper_cuenta wabgpper_cuenta, p.wabgpper_secuencia wabgpper_secuencia , p.wabgpper_secuencia_ren , p.wabgpper_nup wabgpper_nup
 FROM (select  row_number() OVER (partition by wabgpper_entidad , wabgpper_centro_alta , wabgpper_cuenta,wabgpper_secuencia, wabgpper_secuencia_ren order by wabgpper_ordpar asc) as orden,
      wabgpper_entidad ,
	  wabgpper_centro_alta ,
	  wabgpper_cuenta,
	  wabgpper_secuencia,
	  wabgpper_secuencia_ren,
	  wabgpper_ordpar,
	  wabgpper_nup
 	  FROM bi_corp_staging.malbgp_wabgpper
      WHERE partition_date =  "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
            AND  wabgpper_estado NOT IN ('Z', 'P', 'R')
		    AND wabgpper_calpar IN ('TI', 'CT')) p WHERE p.orden=1) per ON
   (a.ipf_ipf_entidad = per.wabgpper_entidad AND
   a.ipf_ipf_centro_alta= per.wabgpper_centro_alta AND
   a.ipf_ipf_cuenta= per.wabgpper_cuenta AND
    a.ipf_ipf_secuencia = per.wabgpper_secuencia AND
    a.ipf_ipf_secuencia_ren = per.wabgpper_secuencia_ren)
LEFT JOIN
(SELECT substr(gen_clave,1,2) as producto,substr(gen_clave,3,4) as subproducto,trim(substr(gen_Datos,20,40)) as descripcion
 FROM bi_corp_staging.tcdtgen
 WHERE gentabla = '0309' and partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}") tcdt039
	ON tcdt039.producto = a.ipf_ipf_producto and tcdt039.subproducto = a.ipf_ipf_subprodu
LEFT JOIN
( SELECT substr(gen_clave,1,2) as canal ,trim(substr(gen_Datos,1,40)) as descripcion
  FROM bi_corp_staging.tcdtgen
  WHERE gentabla = '0222' and partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}" ) tcdt0222
 ON tcdt0222.canal =  a.ipf_ipf_canal_apertura

WHERE a.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
AND  a.ipf_ipf_fecha_opera <= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
    AND a.ipf_ipf_fecha_proven > "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
    AND a.ipf_ipf_ind_estado NOT IN ('Z', 'P', 'R')
    AND suc.partition_date = a.partition_date
    AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}" >= "2020-07-01"

UNION ALL
select
a.ipf_ipf_entidad  cod_pla_entidad ,
cast(a.ipf_ipf_centro_alta as bigint) cod_pla_sucursal ,
trim(suc.desc_comp_centro_op)   ds_pla_sucursal ,
cast(a.ipf_ipf_cuenta as bigint )  cod_pla_cuenta ,
trim(a.ipf_ipf_producto)    cod_pla_producto ,
trim(a.ipf_ipf_subprodu)    cod_pla_subproducto ,
trim(tcdt039.descripcion)  ds_pla_producto ,
trim(a.ipf_ipf_divisa)    cod_pla_divisa,
a.ipf_ipf_secuencia    cod_pla_secuencia ,
cast(per.wabgpper_nup as bigint) cod_per_nup ,
a.ipf_ipf_secuencia_ren    cod_pla_secrenovacion ,
a.ipf_ipf_plazo    fc_pla_plazo ,
trim(a.ipf_ipf_ind_estado)    cod_pla_estado ,
case when a.ipf_ipf_ind_estado = 'A' then 'ACTIVO'
     when a.ipf_ipf_ind_estado = 'P' then 'PREAPERTURA'
     when a.ipf_ipf_ind_estado = 'V' then 'VENCIDO'
     when a.ipf_ipf_ind_estado = 'C' then 'CANECELADO'
     when a.ipf_ipf_ind_estado = 'Z' then 'ANULADO'
     when a.ipf_ipf_ind_estado = 'X' then 'VENCIDO PENDIENTE PAGO'
     when a.ipf_ipf_ind_estado = 'N' then 'RETENIDO'
     when a.ipf_ipf_ind_estado = 'R' then 'RENUMERADO' ELSE NULL END ds_pla_estado,
a.ipf_ipf_saldo_inicial    fc_pla_saldoinicial ,
(a.ipf_ipf_tipoint + a.ipf_ipf_inc_porc_vig)  fc_pla_tasa ,
a.ipf_ipf_canal_apertura    cod_pla_canalapertura ,
trim(tcdt0222.descripcion)    ds_pla_canalapertura ,
a.ipf_ipf_importe_cta    fc_pla_impcta ,
a.ipf_ipf_importe_sin_cta    fc_pla_impsincta ,
a.ipf_ipf_inteabo    fc_pla_inteabo ,
a.ipf_ipf_int_period     fc_pla_intperiodo ,
a.ipf_ipf_fecha_alta    dt_pla_fechaalta ,
a.ipf_ipf_fecha_opera    dt_pla_fechaopera ,
a.ipf_ipf_fecha_proven   dt_pla_fechavencimiento ,
trim(a.ipf_ipf_tarifa_vig)    cod_pla_tarifavig ,
trim(a.ipf_ipf_ind_custodia)     cod_pla_indcustodia,
a.partition_date partition_date


FROM bi_corp_staging.malbgp_bgtcipf a
LEFT JOIN bi_corp_staging.tcdt050 suc ON suc.cod_centro = lpad(a.ipf_ipf_centro_alta, 5, '0')
LEFT JOIN
(SELECT p.wabgpper_entidad wabgpper_entidad , p.wabgpper_centro_alta wabgpper_centro_alta, p.wabgpper_cuenta wabgpper_cuenta, p.wabgpper_secuencia wabgpper_secuencia , p.wabgpper_secuencia_ren , p.wabgpper_nup wabgpper_nup
 FROM (select  row_number() OVER (partition by wabgpper_entidad , wabgpper_centro_alta , wabgpper_cuenta,wabgpper_secuencia, wabgpper_secuencia_ren order by wabgpper_ordpar asc) as orden,
      wabgpper_entidad ,
	  wabgpper_centro_alta ,
	  wabgpper_cuenta,
	  wabgpper_secuencia,
	  wabgpper_secuencia_ren,
	  wabgpper_ordpar,
	  wabgpper_nup
 	  FROM bi_corp_staging.malbgp_wabgpper
      WHERE partition_date =  "2020-07-01"
            AND  wabgpper_estado NOT IN ('Z', 'P', 'R')
		    AND wabgpper_calpar IN ('TI', 'CT')) p WHERE p.orden=1) per ON
   (a.ipf_ipf_entidad = per.wabgpper_entidad AND
   a.ipf_ipf_centro_alta= per.wabgpper_centro_alta AND
   a.ipf_ipf_cuenta= per.wabgpper_cuenta AND
    a.ipf_ipf_secuencia = per.wabgpper_secuencia AND
    a.ipf_ipf_secuencia_ren = per.wabgpper_secuencia_ren)
LEFT JOIN
(SELECT substr(gen_clave,1,2) as producto,substr(gen_clave,3,4) as subproducto,trim(substr(gen_Datos,20,40)) as descripcion
 FROM bi_corp_staging.tcdtgen
 WHERE gentabla = '0309' and partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}") tcdt039
	ON tcdt039.producto = a.ipf_ipf_producto and tcdt039.subproducto = a.ipf_ipf_subprodu
LEFT JOIN
( SELECT substr(gen_clave,1,2) as canal ,trim(substr(gen_Datos,1,40)) as descripcion
  FROM bi_corp_staging.tcdtgen
  WHERE gentabla = '0222' and partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}" ) tcdt0222
 ON tcdt0222.canal =  a.ipf_ipf_canal_apertura

WHERE a.partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
AND  a.ipf_ipf_fecha_opera <= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
    AND a.ipf_ipf_fecha_proven > "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
    AND a.ipf_ipf_ind_estado NOT IN ('Z', 'P', 'R')
    AND suc.partition_date = a.partition_date
    AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}" BETWEEN "2020-06-19" AND "2020-06-30"

UNION ALL

select
cod_pla_entidad  cod_pla_entidad,
cast(cod_pla_sucursal as BIGINT) cod_pla_sucursal,
TRIM(ds_pla_sucursal) ds_pla_sucursal,
CAST(cod_pla_cuenta as BIGINT) cod_pla_cuenta ,
cast(cod_pla_producto as int) cod_pla_producto,
TRIM(cod_pla_subproducto) cod_pla_subproducto ,
TRIM(ds_pla_producto) ds_pla_producto,
TRIM(cod_pla_divisa) cod_pla_divisa,
cast(cod_pla_secuencia as int) cod_pla_secuencia ,
cast(cod_per_nup as bigint) cod_per_nup ,
cast(cod_pla_secrenovacion as int) cod_pla_secrenovacion,
cast(fc_pla_plazo as int) fc_pla_plazo,
cod_pla_estado ,
trim(ds_pla_estado) ds_pla_estado ,
cast(fc_pla_saldoinicial as decimal(15,2)) fc_pla_saldoinicial ,
cast(fc_pla_tasa as decimal(8,5)) fc_pla_tasa,
trim(cod_pla_canalapertura) cod_pla_canalapertura,
trim(ds_pla_canalapertura) ds_pla_canalapertura,
cast(fc_pla_impcta as decimal(12,2)) fc_pla_impcta ,
cast(fc_pla_impsincta as decimal(12,2)) fc_pla_impsincta,
cast(fc_pla_inteabo as decimal(15,2)) fc_pla_inteabo ,
cast(fc_pla_intperiodo as decimal(15,2)) fc_pla_intperiodo,
dt_pla_fechaalta ,
dt_pla_fechaopera ,
dt_pla_fechavencimiento ,
trim(cod_pla_tarifavig) cod_pla_tarifavig,
trim(cod_pla_indcustodia) cod_pla_indcustodia,
partition_date
FROM bi_corp_common.stk_pla_plazofijo_rio35
WHERE partition_date = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}"
AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Plazo_Fijo-Daily') }}" <= "2020-06-18"

  ;