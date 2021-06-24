"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

--- Tabla temporal para traer nup sin repetir ---

WITH cli AS (
select
		nup ,
 	 	 f_alta_registro,
 	     canal_venta,
 	     canal_activacion,
 	     aceptacion_contrato,
 	     aceptac_contrato_viaj,
 	     aceptacion_contrato_tag,
 	     aceptacion_contrato_ext_atm,
 	     adhesion_billetera_todopago,
 	     echeq,
 	     partition_date
 from (select row_number() OVER (partition by nup order by f_alta_registro desc) as orden,
         nup ,
 	 	 f_alta_registro,
 	     canal_venta,
 	     canal_activacion,
 	     aceptacion_contrato,
 	     aceptac_contrato_viaj,
 	     aceptacion_contrato_tag,
 	     aceptacion_contrato_ext_atm,
 	     adhesion_billetera_todopago,
 	     echeq,
 	     partition_date
  	  FROM bi_corp_staging.rio147_hb_client_master
      where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBP-Daily') }}" ) ma where orden=1
)


INSERT OVERWRITE TABLE bi_corp_common.stk_obp_acceso
PARTITION (partition_date)
select
cast (cli.nup as BIGINT)															cod_per_nup,
SUBSTRING(cli.f_alta_registro,1,19)													ts_obp_fecha_alta,
cli.canal_venta											    						ds_obp_canal_venta,
cli.canal_activacion																ds_obp_canal_activacion,
CASE WHEN aceptacion_contrato='1'THEN 1 ELSE 0 END			 						flag_obp_aceptacion_contrato,
CASE WHEN aceptac_contrato_viaj='1'THEN 1 ELSE 0 END			 					flag_obp_aceptacion_contratoviaj,
CASE WHEN aceptacion_contrato_tag= '1' THEN 1 ELSE 0 END							flag_obp_aceptacion_contratotag,
CASE WHEN aceptacion_contrato_ext_atm = '1' THEN 1 ELSE 0 END						flag_obp_aceptacion_contratoextatm,
CASE WHEN adhesion_billetera_todopago= '1' THEN 1 ELSE 0 END 						flag_obp_billetera_todopago	,
CASE WHEN cli.echeq='1' THEN 1 ELSE 0 END											flag_obp_echeq,
SUBSTRING(ses.f_modificacion_registro,1,19)											ts_obp_ultimo_login,
CASE WHEN ses.token ='null' THEN NULL ELSE TRIM(ses.token) END						ds_obp_token,
cli.partition_date																	partition_date

from cli
left join bi_corp_staging.rio147_hb_control_sesion ses on (ses.nup= cli.nup and cli.partition_date= ses.partition_date)


"