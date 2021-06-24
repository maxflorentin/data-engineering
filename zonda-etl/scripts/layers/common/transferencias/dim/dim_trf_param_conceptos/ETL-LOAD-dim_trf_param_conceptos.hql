set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--inserto tabla parametrica con descripciones normalizadas para ATRC-ABAE

INSERT OVERWRITE TABLE bi_corp_common.dim_trf_param_conceptos
select distinct
    'ATRC'                        cod_trf_origen,
    trim(codigo)                  cod_trf_concepto,
    upper(trim(descripcion))      ds_trf_concepto_orig,
    upper(trim(descripcion))      ds_trf_concepto_normalizado,
    '2020-12-11'                  dt_trf_fecha_ult_modif
from bi_corp_staging.atrc_conceptos
where partition_date = '2020-12-11'
union all
select distinct
    'ABAE'                          cod_trf_origen,
    trim(concepto)                  cod_trf_concepto,
    upper(trim(descripcion))        ds_trf_concepto_orig,
    case when upper(trim(descripcion)) = 'APORTE CAPITALES' then 'APORTE DE CAPITAL'
    	 when upper(trim(descripcion)) = 'BIENES REGISTRABLES HABITUALISTAS' then 'BIEN REGISTRABLE HABITUAL'
    	 when upper(trim(descripcion)) = 'BIENES REGISTRABLES NO HABITUALISTAS' then 'BIEN REGISTRABLE NO HABITUAL'
    	 when upper(trim(descripcion)) = 'OPERACIONES INMOBILIARIAS' then 'OPERACION INMOBILIARIA'
    	 when upper(trim(descripcion)) = 'OPERACIONES INMOBILIARIAS HABITUALISTAS' then 'OPERACION INMOBIL HABITUAL'
    	 when upper(trim(descripcion)) = 'REINTEGRO OBRAS SOCIALES Y PREPAGAS' then 'REINTEGRO DE OBRAS SOC Y PREPA'
    	 when upper(trim(descripcion)) = 'SINIESTROS DE SEGUROS' then 'SINIESTRO DE SEGUROS'
    	 when upper(trim(descripcion)) = 'SUSCRIPCION OBLIGACIONES NEGOCIABLES' then 'SUSCRIPCION OBLIGACIONES NEGOC'
    	 	else
    	 		upper(trim(descripcion))
   end 								 ds_trf_concepto_normalizado,
   '2020-12-11'					     dt_trf_fecha_ult_modif
from bi_corp_staging.abae_conceptos
where partition_date = '2020-12-11';