set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

create temporary table tmp_jm_cal_emision as 
   select '23100' as e0665_s1emp,
          '00001' as e0665_id_emisi,
          rpad(esp.isin,40,' ') as e0665_cod_emis,
          lpad(esp.cod_agen, 5, '0')  as e0665_cod_agen,
          case
            when (cast(datediff(nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'),
                                nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.fecha_emision, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'))
                      as float)/365) = 0
            then
              '00000'
            when (cast(datediff(nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'),
                                nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.fecha_emision, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'))
                      as float)/365) < 1
            then
              '00001'
            when (cast(datediff(nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'),
                                nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.fecha_emision, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'))
                      as float)/365) >= 1
             and (cast(datediff(nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'),
                                nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.fecha_emision, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'))
                      as float)/365) <= 5
            then
              '00002'
            when (cast(datediff(nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.maturity, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'),
                                nvl(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(esp.fecha_emision, '31/12/9999') as varchar(10)),"dd/MM/yyyy"))),'9999-12-31'))
                      as float)/365) > 5
            then
              '00003'
            else
              '99999'
          end as e0665_ccodplz,
          '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as e0665_feccali,
          '00000' as e0665_codmercd,
          '9999-12-31' as e0665_fechasta,
          rpad(nvl(esp.cal_consultora,' '), 40, ' ') as e0665_califmae,
          '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as e0665_fecultmo
    from (select *,
                 bond_long_rating_sp cal_consultora,
                 '00001' cod_agen
            from bi_corp_staging.mmff_tactico_especie
           where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
           union all
          select * ,
                 bond_long_rating_moodys cal_consultora,
                 '00002' cod_agen
            from bi_corp_staging.mmff_tactico_especie
           where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
         )esp;

insert overwrite table bi_corp_bdr.jm_cal_emision
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select tmp.e0665_s1emp,
       tmp.e0665_id_emisi,
       tmp.e0665_cod_emis,
       tmp.e0665_cod_agen,
       tmp.e0665_ccodplz,
       case
         when ce1.e0665_cod_emis is null then
           tmp.e0665_feccali
         else
           ce1.e0665_feccali
       end as e0665_feccali,
       tmp.e0665_codmercd,
       tmp.e0665_fechasta,
       tmp.e0665_califmae,
       tmp.e0665_fecultmo
  from tmp_jm_cal_emision tmp
  left join bi_corp_bdr.jm_cal_emision ce1
    on tmp.e0665_s1emp = ce1.e0665_s1emp
   and tmp.e0665_id_emisi = ce1.e0665_id_emisi
   and tmp.e0665_cod_emis = ce1.e0665_cod_emis
   and tmp.e0665_cod_agen = ce1.e0665_cod_agen
   and tmp.e0665_ccodplz = ce1.e0665_ccodplz
   and tmp.e0665_califmae = ce1.e0665_califmae
   and ce1.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}';