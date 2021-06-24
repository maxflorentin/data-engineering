set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.jm_vta_carter partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' as R9736_FEOPERAC
        ,'23100' as R9736_S1EMP
        ,lpad(nvl(cto.id_cto_bdr,0), 9, '0' ) as R9736_CONTRA1
        ,nvl(vta.fecha_venta,'0001-01-01') as R9736_FVTACART
        , case when cast(nvl(hvt.saldo,0) as bigint) >= 0          
            then lpad(regexp_replace(format_number(cast(nvl(hvt.saldo,0) as bigint), 2),"\\,|\\.",""),17,"0")
            else concat("-", lpad(regexp_replace(format_number(cast(nvl(hvt.saldo,0) as bigint), 2),"\\,|\\.|\\-",""),16,"0"))
            end as R9736_IMPPDTE
        , case when cast(nvl(hvt.imp_recuperar,0) as bigint) >= 0          
            then lpad(regexp_replace(format_number(cast(nvl(hvt.imp_recuperar,0) as bigint), 2),"\\,|\\.",""),17,"0")
            else concat("-", lpad(regexp_replace(format_number(cast(nvl(hvt.imp_recuperar,0) as bigint), 2),"\\,|\\.|\\-",""),16,"0"))
            end as R9736_PRECIOOB
        ,'N' as R9736_IND_CREDIT
from    (
            select hvt.idventa
                , sum(cast(hvt.saldo as bigint)) as saldo
                ,sum(cast(hvt.imp_recuperar as bigint)) as imp_recuperar
                ,hvt.partition_date
                ,hvt.cod_centro
                ,hvt.num_contrato
                ,hvt.cod_producto
                ,hvt.cod_subprodu 
            from bi_corp_staging.moria_vc_historico_ventas hvt  
            where hvt.partition_date = '2020-02-19' 
                    and hvt.ind_rechazo = 'N' 
                    and hvt.fecha_alta >= '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'     
                    and hvt.fecha_alta < '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
            group by hvt.idventa
                    ,hvt.partition_date
                    ,hvt.cod_centro
                    ,hvt.num_contrato
                    ,hvt.cod_producto
                    ,hvt.cod_subprodu
        ) hvt 
    left join   
            (
                select vta.idventa
                        ,vta.fecha_venta
                        ,vta.partition_date 
                from bi_corp_staging.moria_vc_ventas vta 
                where vta.partition_date = '2020-02-19'  
                and trim(vta.estado) = 'FINALIZADA'     
            ) vta 
    on hvt.idventa = vta.idventa 
    and hvt.partition_date = vta.partition_date
    left join 
            (
                select 
                    cto.id_cto_bdr
                    ,cto.cred_cod_centro
                    ,cto.cred_num_cuenta
                    ,cto.cred_cod_producto
                    ,cto.cred_cod_subprodu_altair
                from bi_corp_bdr.normalizacion_id_contratos cto 
                where cto.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'  
            ) cto
    on  cto.cred_cod_centro = hvt.cod_centro
        and cto.cred_num_cuenta = hvt.num_contrato
        and cto.cred_cod_producto = hvt.cod_producto
        and cto.cred_cod_subprodu_altair = hvt.cod_subprodu