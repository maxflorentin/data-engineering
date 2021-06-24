SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_cla_clavedigital
PARTITION (partition_date)

select
DISTINCT trim(entidad)				cod_cla_entidad,
cast(nup as bigint)			cod_per_nup,
case when trim(sesion)='' then null
	 else trim(sesion) end		cod_cla_sesion,
case when trim(fase)='' then null
	 else trim(fase) end		ds_cla_fase,
case when trim(sysid)='' then null
	 else trim(sysid) end		cod_cla_sysid,
case when trim(task)='' then null
	 else trim(task) end		cod_cla_tarea,
case when trim(trmid)='' then null
	 else trim(trmid) end		cod_cla_trmid,
case when trim(userid)='' then null
	 else trim(userid) end		cod_cla_userid,
case when trim(trnid)='' then null
	 else trim(trnid) end		cod_cla_trnid,
case when trim(servicio)='' then null
	 else trim(servicio) end		ds_cla_servicio,
case when trim(canal)='' then null
	 else trim(canal) end		cod_cla_canal,
case when trim(subcanal)='' then null
	 else trim(subcanal) end		cod_cla_subcanal,
case when trim(ret_code)='' then null
	 else trim(ret_code) end		cod_cla_retorno,
case when trim(ret_texto)='' then null
	 else trim(ret_texto) end		ds_cla_ret_texto,
case when trim(stamp)='' then null
	else  CONCAT(SUBSTRING(trim(stamp),1,10),' ' ,TRANSLATE(SUBSTRING(trim(stamp),12,8),'.',':')) end	ts_cla_fecha,
case when trim(SUBSTRING(detalle1,0,2))=''
	 then null else SUBSTRING(detalle1,0,2) end					cod_cla_sw_rc,
case when trim(SUBSTRING(detalle1,3,1))=''
	 then null else SUBSTRING(detalle1,3,1) end						cod_cla_sw_ciclo,
case when trim(SUBSTRING(detalle1,4,1))=''
	 then null else SUBSTRING(detalle1,4,1) end						cod_cla_sw_sistema,
case when trim(SUBSTRING(detalle1,5,1))=''
	 then null else SUBSTRING(detalle1,5,1) end						cod_cla_sw_preguntas,
case when trim(SUBSTRING(detalle1,6,1))=''
	 then null else SUBSTRING(detalle1,6,1) end					cod_cla_sw_autenticacion,
case when trim(SUBSTRING(detalle1,7,1))=''
	 then null else SUBSTRING(detalle1,7,1) end					cod_cla_sw_pin,
case when trim(SUBSTRING(detalle1,8,1))=''
	 then null else SUBSTRING(detalle1,8,1) end					cod_cla_sw_cvv2,
case when trim(SUBSTRING(detalle1,9,1))=''
	 then null else SUBSTRING(detalle1,9,1) end					cod_cla_sw_otp,
case when trim(SUBSTRING(detalle1,10,4))=''
	 then null else SUBSTRING(detalle1,10,4) end					cod_cla_sw_tipo,
case when trim(SUBSTRING(detalle1,14,20))=''
	 then null else SUBSTRING(detalle1,14,20) end						cod_cla_sw_numero,
case when trim(SUBSTRING(detalle1,34,11))=''
	 then null else CAST(SUBSTRING(detalle1,34,11) AS BIGINT) end						ds_cla_nrodoc,
case when trim(SUBSTRING(detalle1,45,8))=''  or trim(SUBSTRING(detalle1,45,8))= '0001-01-01'
	 then null else SUBSTRING(detalle1,45,8) end					ds_cla_fecha_nac,
case when trim(SUBSTRING(concat(detalle1,detalle2),53,2))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),53,2) end	cod_cla_pd_rc,
case when trim(SUBSTRING(concat(detalle1,detalle2),55,1))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),55,1) end	cod_cla_pd_ciclo,
case when trim(SUBSTRING(concat(detalle1,detalle2),56,3))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),56,3) end	cod_cla_pd,
case when trim(SUBSTRING(concat(detalle1,detalle2),59,26))=''
	 then null else
	 CONCAT(SUBSTRING(concat(detalle1,detalle2),59,10),' ' ,TRANSLATE(SUBSTRING(concat(detalle1,detalle2),70,8),'.',':')) end	ts_cla_pd_fecha,
case when trim(SUBSTRING(concat(detalle1,detalle2),85,2))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),85,2) end	cod_pla_au_rc,
case when trim(SUBSTRING(concat(detalle1,detalle2),87,1))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),87,1) end	cod_pla_au_ciclo,
case when trim(SUBSTRING(concat(detalle1,detalle2),88,4))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),88,4) end	cod_pla_au_tipo,
case when trim(SUBSTRING(concat(detalle1,detalle2),92,20))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),92,20) end	cod_pla_au_numero,
case when trim(SUBSTRING(concat(detalle1,detalle2),102,4))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),102,4) end	cod_pla_au_offset,
case when trim(SUBSTRING(concat(detalle1,detalle2),106,2))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),106,2) end	cod_pla_ck_rc,
case when trim(SUBSTRING(concat(detalle1,detalle2),108,1))=''
	 then null else SUBSTRING(concat(detalle1,detalle2),108,1) end	cod_pla_ck_ciclo,
concat(detalle1,detalle2)											ds_cla_detalle,
partition_Date

from bi_corp_staging.sgi1_claves_sr_canales
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}";
