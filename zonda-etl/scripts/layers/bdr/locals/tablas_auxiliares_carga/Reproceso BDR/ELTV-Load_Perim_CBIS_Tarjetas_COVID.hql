set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


    --**********************************************************************************--
    --*           Carga del perímetro para Contratos COVID para Tarjetas               *--
    --**********************************************************************************--
    --* Input:                                                                         *--
    --*  --bi_corp_bdr.jm_contr_bis                                                    *--
    --*                                                                                *--
    --*                                                                                *--
    --* Detalle:                                                                       *--
    --*  --TASA 0 , EARLYCARDS, PLAN V                           					   *--
    --*                                                                                *--
    --*                                                                                *--
    --*  -- Fecha               : 06/04/2021                                           *--
    --*  -- Autor               : Juan Hung                                            *--
    --*  -- Modificaciones      :                                                      *--
    --*  -- Observaciones       : La fecha de la partición de la tabla debe ser desde  *--
    --*                            2020-01 cuando empezaron las moratorias             *--
    --*  --                                                                            *--
    --*                                                                                *--
    --**********************************************************************************--


--Perímetro Cotratos Bis Historico
insert overwrite table bi_corp_bdr.perim_contratos_bis_tarjetas_covid 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select
		nch.cred_cod_centro
		,nch.cred_num_cuenta
		,nch.cred_cod_producto
		,nch.cred_cod_subprodu_altair
		,cbi.g4095_contra1 
		,cbi.g4095_fechaper 
		,cbi.g4095_feccance 
		,cbi.g4095_coddiv 
		,cbi.contr_partition_date
from
	(
	select
		cbi.g4095_contra1 ,
		cbi.g4095_fechaper ,
		cbi.g4095_feccance ,
		cbi.g4095_coddiv ,
		min(cbi.partition_date) as contr_partition_date
	from
		bi_corp_bdr.jm_contr_bis cbi
	where
		cbi.partition_date >= '2020-01'
	group by
		cbi.g4095_contra1 ,
		cbi.g4095_fechaper ,
		cbi.g4095_feccance ,
		cbi.g4095_coddiv 
	) cbi
inner JOIN bi_corp_bdr.normalizacion_id_contratos_history nch on
	cbi.g4095_contra1 = nch.id_cto_bdr;
 