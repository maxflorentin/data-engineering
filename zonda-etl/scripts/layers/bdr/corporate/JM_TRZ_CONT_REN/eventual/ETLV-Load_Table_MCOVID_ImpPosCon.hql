set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT INTO table bi_corp_bdr.jm_posic_contr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}')
select  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}'  AS  E0621_FEOPERAC
        ,'23100' AS E0621_S1EMP
        ,lpad(ncr.id_cto_bdr , 9, '0')  AS E0621_CONTRA1 --Normalización de Contrato
        ,rpad(' ' , 40, ' ') AS E0621_CTA_CONT
        ,case when cov.tipo ='EARLYCARDS' then '99997'
              when cov.tipo ='PLAN V' then '99998'
              when cov.tipo ='TASA 0' then '99999'
         end 
         AS e0621_tip_impt 
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}'  AS e0621_fec_mes
        ,rpad(' ' , 40, ' ') AS E0621_AGRCTACB
        ,case when cov.saldo_hoy  >= 0
            then concat("-", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_hoy, 0), 2),"\\,|\\.",""),"\\,|\\.","") ,16,"0") )
            else concat("+", lpad(regexp_replace(regexp_replace(format_number(coalesce (cov.saldo_hoy, 0), 2),"\\,|\\.",""),"\\,|\\.|\\-",""),16,"0") ) 
         end as E0621_IMPORTH   
        ,rpad(cbi.g4095_coddiv ,3,' ') as E0621_CODDIV
        ,'0001-01-01' as e0621_fecultmo 
        ,lpad(' ', 40, ' ') as E0621_CENTCTBL
        ,rpad(' ' , 40, ' ') as E0621_CTACGBAL
from 
(         
            select cov.sucursal
                    ,cov.nro_cuenta
                    ,cov.codigo_producto
                    ,cov.codigo_subproducto
                    ,sum(cov.saldo_original) as saldo_original
                    ,sum(cov.saldo_hoy)  as saldo_hoy
                    ,cov.tipo
                    ,cov.moneda 
            from 
                (
                    select distinct cov.sucursal
                                    ,cov.nro_cuenta
                                    ,cov.codigo_producto
                                    ,cov.codigo_subproducto
                                    ,cast(cov.saldo_original as double) as saldo_original
                                    ,cast(cov.saldo_hoy as double) as saldo_hoy
                                    ,cov.moneda 
                                    ,cov.tipo
                                    ,SUBSTRING(cov.fecha_alta_producto,1,10) as fecha_alta_producto
                                    ,SUBSTRING(cov.fecha_vencimiento_producto,1,10) as fecha_vencimiento_producto
                    from bi_corp_bdr.saldos_tarjetas_covid cov
                    where cov.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_saldos_tarjetas_covid', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}'
                        and tipo in ('EARLYCARDS','PLAN V','TASA 0')              
        
                ) cov
                group by cov.sucursal
                    ,cov.nro_cuenta
                    ,cov.codigo_producto
                    ,cov.codigo_subproducto
                    ,cov.tipo
                    ,cov.moneda             
) cov
inner join 
(
    select ncr.id_cto_bdr
            ,ncr.cred_cod_entidad
            ,ncr.cred_cod_centro
            ,ncr.cred_num_cuenta
            ,ncr.cred_cod_producto
            ,ncr.cred_cod_subprodu_altair
    from bi_corp_bdr.normalizacion_id_contratos ncr
    where ncr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}'
) ncr
on 
 lpad(cast(trim(cov.sucursal) as string) ,4,'0') = ncr.cred_cod_centro
and lpad(cast(trim(cov.nro_cuenta) as string),12,'0') = ncr.cred_num_cuenta 
and lpad(cast(trim(cov.codigo_producto) as string),2,'0') = ncr.cred_cod_producto
and cast(trim(cov.codigo_subproducto)  as string) = ncr.cred_cod_subprodu_altair
inner join 
(
    select cbi.g4095_contra1
            ,cbi.g4095_fechaper
            ,cbi.g4095_coddiv
            ,cbi.g4095_feccance 
            ,cbi.g4095_idpro_lc
    from bi_corp_bdr.jm_contr_bis cbi
    where cbi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Renumeracion_IPC_COVID') }}'
) cbi 
on cbi.g4095_contra1 = ncr.id_cto_bdr
and trim(cov.moneda) = trim(cbi.g4095_coddiv);