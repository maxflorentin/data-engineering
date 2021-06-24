create external table if not exists bi_corp_bdr.perim_ifrs9_tablon_prov (
            t_cod_entidad string,
            t_cod_centro string,
            t_num_cuenta string,
            t_cod_producto string,
            t_cod_subprodu_altair string,
            s_provision_in_balance_bdr string,
            s_provision_out_balance_bdr string,
            s_provision_amount_bdr string,
            s_rubro_cargabal_in_provision string,
            s_rubro_cargabal_out_provision string,
            s_rubro_cargabal_provision string,
            s_final_stage string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_ifrs9_tablon_prov';


create external table if not exists bi_corp_bdr.perim_ifrs9_tablon_prov_mmff (
            t_idf_cto_ifrs string,
            s_provision_in_balance_bdr string,
            s_provision_out_balance_bdr string,
            s_provision_amount_bdr string,
            s_rubro_cargabal_in_provision string,
            s_rubro_cargabal_out_provision string,
            s_rubro_cargabal_provision string,
            s_final_stage string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_ifrs9_tablon_prov_mmff';
