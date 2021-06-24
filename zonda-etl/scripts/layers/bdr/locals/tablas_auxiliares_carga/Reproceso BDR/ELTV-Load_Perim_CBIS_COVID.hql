set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


    --**********************************************************************************--
    --*                     Carga del perímetro para Contratos COVID                   *--
    --**********************************************************************************--
    --* Input:                                                                         *--
    --*  --bi_corp_bdr.jm_contr_bis                                                    *--
    --*                                                                                *--
    --*                                                                                *--
    --* Detalle:                                                                       *--
    --*  --FOGAR NÓMINA, MIPYME PLUS, ATP, PRESTAMO FLEXIBLE                           *--
    --*                                                                                *--
    --*                                                                                *--
    --*  -- Fecha               : 25/01/2021                                           *--
    --*  -- Autor               : Juan Hung                                            *--
    --*  -- Modificaciones      :                                                      *--
    --*  -- Observaciones       : La fecha de la partición de la tabla debe ser desde  *--
    --*                            2020-01 cuando empezaron las moratorias             *--
    --*  --                                                                            *--
    --*                                                                                *--
    --**********************************************************************************--


--Fogar Nómina

with cont_bis as (
select cbi.g4095_contra1
        ,nch.cred_cod_entidad
        ,nch.cred_cod_centro
        ,nch.cred_num_cuenta
        ,nch.cred_cod_producto
        ,nch.cred_cod_subprodu_altair
        ,cbi.g4095_fechaper
        ,cbif.g4095_fecvento
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,cbi.partition_date as contr_partition_date
from 
            (
                select cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
                        ,min(cbi.partition_date) as partition_date
                from   bi_corp_bdr.jm_contr_bis cbi
                where cbi.partition_date >= '2020-01'
                and cbi.g4095_idfinald = '39084'        
                group by cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
            ) cbi
            left join 
            (
                select cbi.g4095_contra1
                        ,max(cbi.g4095_fecvento ) as g4095_fecvento
                from bi_corp_bdr.jm_contr_bis cbi     
                where cbi.partition_date >= '2020-01'
                and cbi.g4095_idfinald = '39084'
                group by cbi.g4095_contra1
            ) cbif
            on cbi.g4095_contra1 = cbif.g4095_contra1
            inner JOIN bi_corp_bdr.normalizacion_id_contratos_history nch
            on cbi.g4095_contra1 = nch.id_cto_bdr
) 
insert overwrite table bi_corp_bdr.perim_contratos_bis_covid 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select  cbi.g4095_contra1
        ,cbi.g4095_fechaper
        ,cbi.g4095_fecvento
        ,core.pefecest
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,'Fogar' as desc_tipo
        ,cbi.contr_partition_date
from cont_bis cbi
            left join 
            (
                select *
                FROM bi_corp_staging.malpe_pedt042 core
                WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and core.PEESTOPE = 'C'
            ) core
            on core.pecdgent = cbi.cred_cod_entidad
            AND core.pecodofi = cbi.cred_cod_centro
            AND core.penumcon = cbi.cred_num_cuenta
            AND core.pecodpro = cbi.cred_cod_producto
            AND trim(core.pecodsub) = cbi.cred_cod_subprodu_altair;
 
--MiPyme Plus
with cont_bis as (
select cbi.g4095_contra1
        ,nch.cred_cod_entidad
        ,nch.cred_cod_centro
        ,nch.cred_num_cuenta
        ,nch.cred_cod_producto
        ,nch.cred_cod_subprodu_altair
        ,cbi.g4095_fechaper
        ,cbif.g4095_fecvento
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,cbi.partition_date as contr_partition_date
from 
            (
                select cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
                        ,min(cbi.partition_date) as partition_date
                from   bi_corp_bdr.jm_contr_bis cbi
                where cbi.partition_date >= '2020-01'
                and cbi.g4095_idfinald in ('35997', '39132')       
                group by cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
            ) cbi
            left join 
            (
                select cbi.g4095_contra1
                        ,max(cbi.g4095_fecvento ) as g4095_fecvento
                from bi_corp_bdr.jm_contr_bis cbi     
                where cbi.partition_date >= '2020-01'
                and cbi.g4095_idfinald in ('35997', '39132')  
                group by cbi.g4095_contra1
            ) cbif
            on cbi.g4095_contra1 = cbif.g4095_contra1
            inner JOIN bi_corp_bdr.normalizacion_id_contratos_history nch
            on cbi.g4095_contra1 = nch.id_cto_bdr
) 
insert into table bi_corp_bdr.perim_contratos_bis_covid 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select  cbi.g4095_contra1
        ,cbi.g4095_fechaper
        ,cbi.g4095_fecvento
        ,core.pefecest
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,'MiPyme' as desc_tipo
        ,cbi.contr_partition_date
from cont_bis cbi
            left join 
            (
                select *
                FROM bi_corp_staging.malpe_pedt042 core
                WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and core.PEESTOPE = 'C'
            ) core
            on core.pecdgent = cbi.cred_cod_entidad
            AND core.pecodofi = cbi.cred_cod_centro
            AND core.penumcon = cbi.cred_num_cuenta
            AND core.pecodpro = cbi.cred_cod_producto
            AND trim(core.pecodsub) = cbi.cred_cod_subprodu_altair;
        
     
--ATP   
with cont_bis as (
select cbi.g4095_contra1
        ,nch.cred_cod_entidad
        ,nch.cred_cod_centro
        ,nch.cred_num_cuenta
        ,nch.cred_cod_producto
        ,nch.cred_cod_subprodu_altair
        ,cbi.g4095_fechaper
        ,cbif.g4095_fecvento
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,cbi.partition_date as contr_partition_date
from 
            (
                select cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
                        ,min(cbi.partition_date) as partition_date
                from   bi_corp_bdr.jm_contr_bis cbi
                where cbi.partition_date >= '2020-01'
                    and cbi.g4095_idfinald in ('39133', '39134' , '39135')
                    and cbi.g4095_idpro_lc = '39139' -- cod_baremo_local = '39139'   cod_baremo_alfanumerico_local ='390112'       
                group by cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
            ) cbi
            left join 
            (
                select cbi.g4095_contra1
                        ,max(cbi.g4095_fecvento ) as g4095_fecvento
                from bi_corp_bdr.jm_contr_bis cbi     
                where cbi.partition_date >= '2020-01'
                    and cbi.g4095_idfinald in ('39133', '39134' , '39135')
                    and cbi.g4095_idpro_lc = '39139' -- cod_baremo_local = '39139'   cod_baremo_alfanumerico_local ='390112'   
                group by cbi.g4095_contra1
            ) cbif
            on cbi.g4095_contra1 = cbif.g4095_contra1
            inner JOIN bi_corp_bdr.normalizacion_id_contratos_history nch
            on cbi.g4095_contra1 = nch.id_cto_bdr
) 
insert into table bi_corp_bdr.perim_contratos_bis_covid 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select  cbi.g4095_contra1
        ,cbi.g4095_fechaper
        ,cbi.g4095_fecvento
        ,core.pefecest
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,'ATP' as desc_tipo
        ,cbi.contr_partition_date
from cont_bis cbi
            left join 
            (
                select *
                FROM bi_corp_staging.malpe_pedt042 core
                WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and core.PEESTOPE = 'C'
            ) core
            on core.pecdgent = cbi.cred_cod_entidad
            AND core.pecodofi = cbi.cred_cod_centro
            AND core.penumcon = cbi.cred_num_cuenta
            AND core.pecodpro = cbi.cred_cod_producto
            AND trim(core.pecodsub) = cbi.cred_cod_subprodu_altair;
        
        
--Préstamo Flexible         

        
with cont_bis as (
select cbi.g4095_contra1
        ,nch.cred_cod_entidad
        ,nch.cred_cod_centro
        ,nch.cred_num_cuenta
        ,nch.cred_cod_producto
        ,nch.cred_cod_subprodu_altair
        ,cbi.g4095_fechaper
        ,cbif.g4095_fecvento
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,cbi.partition_date as contr_partition_date
from 
            (
                select cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
                        ,min(cbi.partition_date) as partition_date
                from   bi_corp_bdr.jm_contr_bis cbi
                where cbi.partition_date >= '2020-01'
                    and cbi.g4095_idfinald = '39055'
                    and cbi.g4095_idpro_lc in('39005','39026','39033','39066')       
                group by cbi.g4095_contra1
                        ,cbi.g4095_fechaper
                        ,cbi.g4095_coddiv
                        ,cbi.g4095_idfinald
            ) cbi
            left join 
            (
                select cbi.g4095_contra1
                        ,max(cbi.g4095_fecvento ) as g4095_fecvento
                from bi_corp_bdr.jm_contr_bis cbi     
                where cbi.partition_date >= '2020-01'
                    and cbi.g4095_idfinald = '39055'
                    and cbi.g4095_idpro_lc in('39005','39026','39033','39066')   
                group by cbi.g4095_contra1
            ) cbif
            on cbi.g4095_contra1 = cbif.g4095_contra1
            inner JOIN bi_corp_bdr.normalizacion_id_contratos_history nch
            on cbi.g4095_contra1 = nch.id_cto_bdr
) 
insert into table bi_corp_bdr.perim_contratos_bis_covid 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select  cbi.g4095_contra1
        ,cbi.g4095_fechaper
        ,cbi.g4095_fecvento
        ,core.pefecest
        ,cbi.g4095_coddiv
        ,cbi.g4095_idfinald 
        ,'PresFlex' as desc_tipo
        ,cbi.contr_partition_date
from cont_bis cbi
            left join 
            (
                select *
                FROM bi_corp_staging.malpe_pedt042 core
                WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and core.PEESTOPE = 'C'
            ) core
            on core.pecdgent = cbi.cred_cod_entidad
            AND core.pecodofi = cbi.cred_cod_centro
            AND core.penumcon = cbi.cred_num_cuenta
            AND core.pecodpro = cbi.cred_cod_producto
            AND trim(core.pecodsub) = cbi.cred_cod_subprodu_altair;
                
        

        

    