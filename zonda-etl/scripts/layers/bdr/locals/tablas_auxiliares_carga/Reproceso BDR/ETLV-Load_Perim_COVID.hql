set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
 
 
with  contrato_org as
 (
 SELECT distinct entidad
            ,concat('0',substring(num_propues,4,3)) as oficina
            ,concat('0',substring(num_propues,7,11)) as cuenta
            ,substring(num_propues,1,17)  as num_propues
    FROM bi_corp_staging.malug_ugdtmae
    where num_propues like 'COV%'
    and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malug_ugdtmae', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
) 
INSERT overwrite table bi_corp_bdr.perim_contratos_covid
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select corg.entidad
        ,corg.oficina
        ,corg.cuenta        
        , df.producto
        , df.subpro 
        ,corg.num_propues
        ,substring(df.num_propues,18,3) as cuotas
        ,df.impconce
        ,df.feforma
        ,df.fecha_pago
        ,df.divisa
        ,corg.cuenta as Cuenta_Org
        ,'CONTR_ORG' as tipo
    FROM bi_corp_staging.malug_ugdtmae df inner join contrato_org corg 
        on df.oficina = corg.oficina
        and df.cuenta = corg.cuenta 
        and df.entidad = corg.entidad 
    where df.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malug_ugdtmae', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
union all
 SELECT     entidad
            ,oficina
            ,cuenta
            ,producto 
            ,subpro 
            ,substring(num_propues,1,17) as num_propues
            ,substring(num_propues,18,3) as cuotas
            ,impconce
            ,feforma
            ,fecha_pago
            ,divisa
            ,concat('0',substring(num_propues,7,11)) as Cuenta_Org
            ,'COVID' as tipo
    FROM bi_corp_staging.malug_ugdtmae
    where num_propues like 'COV%'
    and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malug_ugdtmae', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}';
   