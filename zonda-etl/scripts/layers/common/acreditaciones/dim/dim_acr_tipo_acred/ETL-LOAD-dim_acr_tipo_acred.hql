set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 4000000;
set hive.merge.smallfiles.avgsize = 16000000;
set hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.job.queue.name=root.dataeng;

--inserto manualmente los distintos sistemas con su tipo de acreditacion
 INSERT
	OVERWRITE TABLE bi_corp_common.dim_acr_tipo_acred PARTITION (partition_date)
select
	t.*,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Acreditaciones') }}' partition_date
from
	(
	select
		stack( 23,
		    'AMEP HAB'  ,	'AMEP'							 ,'PS',		'PLAN SUELDO',
            'ANET'      ,	'ANET'							 ,'PS',		'PLAN SUELDO',
            'ANSES JUB' ,	'ANSES'						 	 ,'HA',		'HABERES',
            'ATP HAB'   ,	'ASISTENCIA TRABAJO PRODUCCION'  ,'PS',		'PLAN SUELDO',
            'ATRC HAB'  ,	'ATRC'							 ,'PS',		'PLAN SUELDO',
            'ATRC JUB'  ,	'ATRC'							 ,'HA',		'HABERES',
            'CCI HAB'   ,	'ACREDITACIONES CITI'			 ,'PS',		'PLAN SUELDO',
            'EMP.BCO.'  ,	'EMPLEADOS SANTANDER'            ,'PS',		'PLAN SUELDO',
            'HOMEB. HAB',	'ONLINE BANKING'			 	 ,'PS',		'PLAN SUELDO',
            'HOMEB. HON',	'ONLINE BANKING'       			 ,'PH',		'HONORARIOS',
            'MANUAL HAB',	'PAGOS MANUALES'				 ,'PS',		'PLAN SUELDO',
            'MANUAL HON',	'PAGOS MANUALES'				 ,'PH',		'HONORARIOS',
            'MANUAL VOL',	'PAGOS MANUALES'				 ,'PV',		'PAGOS VOLUNTARIOS',
            'MANUALES'  ,	'PAGOS MANUALES'				 ,'PS',		'PLAN SUELDO',
            'NETBAN HAB',	'NETBANKING'					 ,'PS',		'PLAN SUELDO',
            'NETBAN HON',	'NETKANKING'					 ,'PH',		'HONORARIOS',
            'PIRYP HAB' ,	'PIRYP'						     ,'PS',		'PLAN SUELDO',
            'PIRYP HON' ,	'PIRYP' 						 ,'PH',		'HONORARIOS',
            'PIRYP HP'  ,	'PIRYP'						     ,'HA',		'HABERES',
            'PIRYP VOL' ,	'PIRYP'						     ,'PV',		'PAGOS VOLUNTARIOS',
            'PROVE HAB' ,	'PAGO A PROVEEDORES'			 ,'PS',		'PLAN SUELDO',
            'SECPRV HAB',	'SECTOR PRIVADO'				 ,'PS',		'PLAN SUELDO',
            'SECPUB HAB',	'SECTOR PUBLICO'				 ,'PS',		'PLAN SUELDO'
        )
    )t;