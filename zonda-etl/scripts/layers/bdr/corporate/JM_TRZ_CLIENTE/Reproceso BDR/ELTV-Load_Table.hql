set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.jm_trz_cliente
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select  '23100' as G7015_S1EMP
        ,lpad(cbb.g4093_idnumcli, 9, '0') as G7015_IDNUMCLI
        ,cbb.g4093_fchini G7015_FECDESDE
        ,lpad('10000', 9, '0')  as G7015_IDSISTE
        ,rpad(' ', 1, ' ') as G7015_TIP_PER
        ,lpad('0', 9, '0') G7015_CDG_PERS
        ,lpad(ctp.shortname, 50, ' ') as G7015_CODSISTE
        ,cbb.g4093_fchfin  as G7015_FEC_HAS
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  as G7015_FEC_MOD
from         
(
    select cbb.g4093_idnumcli
            ,cbb.g4093_fchini
            ,cbb.g4093_fchfin 
    from bi_corp_bdr.jm_client_bii cbb 
     where cbb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'   
    and g4093_clisegm in ('00001','00002','00008','00011','00017' ) 
) cbb
inner join
(
    select mkg.nup,
            trim(mkg.shortname) as shortname
    from bi_corp_bdr.perim_mdr_contraparte mkg
    where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'    
) ctp  
on lpad(trim(ctp.nup),9,'0') = trim(cbb.g4093_idnumcli);