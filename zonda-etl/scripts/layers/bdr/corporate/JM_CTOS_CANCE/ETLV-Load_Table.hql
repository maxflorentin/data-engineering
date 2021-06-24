"set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
set hive.cbo.enable=true;


insert overwrite table bi_corp_bdr.jm_ctos_cance
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')

SELECT distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AS H0711_FEOPERAC,
       lpad(nvl(ble.cod_baremo_global,'0'),5,'0') AS H0711_S1EMP,
       lpad(cmap.id_cto_bdr, 9, "0") AS H0711_CONTRA1,
       lpad(core.PEMOTEST, 5, "0") AS H0711_MOTVCANC,
       '0001-01-01' AS H0711_FECULTMO
FROM bi_corp_staging.malpe_pedt042 core
inner JOIN bi_corp_bdr.normalizacion_id_contratos cmap
on core.pecdgent = cmap.cred_cod_entidad
AND core.pecodofi = cmap.cred_cod_centro
AND core.penumcon = cmap.cred_num_cuenta
AND core.pecodpro = cmap.cred_cod_producto
AND trim(core.pecodsub) = cmap.cred_cod_subprodu_altair
and cmap.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
inner JOIN
          (select cod_producto
             from santander_business_risk_arda.maestro_atributos
            where data_date_part = "20190930"
           GROUP BY cod_producto
           ) ma
               on core.pecodpro = ma.cod_producto
LEFT JOIN bi_corp_bdr.jm_trz_cont_ren ren
     on ren.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
    and ren.g7025_cont_ant = cmap.id_cto_bdr
    AND ren.G7025_MOTRENU not in ('00020','00060', '00061', '00062','00063','00064','00065','00066','00067') --Código de renumeraciones que se den de alta! (Para covid) Motivo de renumeración
left join
          (select empresa, cod_baremo_local, cod_baremo_global
             from bi_corp_bdr.map_baremos_global_local
             where cod_negocio_local = '1'
               and '{{ var.json.jm_ctos_cance.baremos_global_local_lcd }}'
            between fecha_desde and fecha_hasta) ble
                 on ble.cod_baremo_local = core.pecdgent
WHERE core.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Monthly') }}'
and core.PEESTOPE = 'C'
and ren.g7025_cont_ant is null
and substr(core.PEFECEST,1,7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
union all
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AS H0711_FEOPERAC,
       '23100' AS H0711_S1EMP,
       lpad(cmap.id_cto_bdr, 9, "0") AS H0711_CONTRA1,
       lpad(999, 5, "0") AS H0711_MOTVCANC,
       '0001-01-01' AS H0711_FECULTMO
  from santander_business_risk_arda.contratos con
inner JOIN bi_corp_bdr.normalizacion_id_contratos cmap
     on con.cod_entidad = cmap.cred_cod_entidad
    AND con.cod_centro = cmap.cred_cod_centro
    AND con.num_cuenta = cmap.cred_num_cuenta
    AND con.cod_producto = cmap.cred_cod_producto
    AND con.cod_subprodu_altair = cmap.cred_cod_subprodu_altair
    and cmap.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
 where con.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly') }}'
   and con.cod_marca = "FA"
   and substr(con.fec_castigo,1,7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}';"
