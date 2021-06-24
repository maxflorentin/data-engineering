set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.perim_ifrs9_tablon_prov 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select 
            a.t_cod_entidad
            ,a.t_cod_centro
            ,a.t_num_cuenta
            ,a.t_cod_producto
            ,a.t_cod_subprodu_altair
            ,COALESCE(cast(a.s_provision_in_balance_bdr as double),0) as s_provision_in_balance_bdr
            ,COALESCE(cast(a.s_provision_out_balance_bdr as double),0) as s_provision_out_balance_bdr
            ,COALESCE(cast(a.s_provision_amount_bdr as double),0) as s_provision_amount_bdr
            ,a.s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,s_rubro_cargabal_provision
            ,a.s_final_stage
        from santander_business_risk_ifrs9.ifrs9_tablon a
        where cast(a.periodo as string) = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_ifrs9_tablon', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
        AND a.e_portfolio_ifrs9 in ("HIPOTECARIOS","PRENDARIOS", "PERSONALES", "TARJETAS",
                                    "CUENTA CORRIENTE", "OTROS","PYME 1 PF","PYME 1 PJ","PYME 2","EMPRESAS","INSTITUCIONES",
                                    "CORPORATE (GCB)");

--MMFF                                
insert overwrite table bi_corp_bdr.perim_ifrs9_tablon_prov_mmff 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select 
            a.t_idf_cto_ifrs
            ,COALESCE(cast(a.s_provision_in_balance_bdr as double),0) as s_provision_in_balance_bdr
            ,COALESCE(cast(a.s_provision_out_balance_bdr as double),0) as s_provision_out_balance_bdr
            ,COALESCE(cast(a.s_provision_amount_bdr as double),0) as s_provision_amount_bdr
            ,s_rubro_cargabal_in_provision
            ,s_rubro_cargabal_out_provision
            ,a.s_rubro_cargabal_provision 
            ,a.s_final_stage 
        from santander_business_risk_ifrs9.ifrs9_tablon a
        where cast(a.periodo as string) = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_ifrs9_tablon', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
        AND a.e_portfolio_ifrs9 in ('CORPORATE (GCB)','BONOS');