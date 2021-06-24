CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_cantidad_operaciones_otra_sucursal AS
select count(*) as cant_ope
       ,sum(imp_total_chq_ok) as tot_imp_ok
       ,sum(cant_chq_ok) as tot_chq_ok
       ,sum(imp_total_chq_rech) as tot_imp_rech
       ,sum(cant_chq_rech) as tot_chq_rech
      ,case tpo_linea
        when 'C' then 'carterizado'
        when 'E' then 'estandarizado'
        when 'X' then 'mayorista'
      end as cliente
       ,t030.pesegcal as segmento
       ,fec_tramite
       ,des_est_tramite as estado
        ,cgr.cod_nup
        ,cgr.suc_cliente
        ,cgr.cuenta_cli
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malpe_pedt030 t030 on cgr.cod_nup = t030.penumper
join bi_corp_staging.malzx_alzxlsta sta on cgr.cod_est_tramite = sta.cod_est_tramite
where suc_cliente <> suc_recepcion and cgr.cod_est_tramite in ('22', '23', '24', '25', '26')
 and t030.pecdgent= '0072' and cgr.cod_nup not in ('00199863', '00199865', '03846180')
group by banca_cli, tpo_linea, t030.pesegcal, fec_tramite,
sta.des_est_tramite, cgr.cod_nup, cgr.suc_cliente, cgr.cuenta_cli;