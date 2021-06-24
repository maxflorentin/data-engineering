CREATE VIEW IF NOT EXISTS view_cheques_operaciones_rechazadas_desestimadas.hql AS
select count(*) as cant_ope
       ,sum(imp_total_chq) as monto_tot
       ,sum(cant_chq) as cant_chq_tot
       ,sum(imp_total_chq_ok) as monto_tot_ok
       ,sum(cant_chq_ok) as cant_chq_tot_2
       ,sum(imp_total_chq_rech) as monto_rech
       ,sum(cant_chq_rech) as cant_cheques_rech
       ,case tpo_linea
         when 'C' then 'carterizado'
         when 'E' then 'estandarizado'
         when 'X' then 'mayorista'
       end as cliente
       ,t030.pesegcal as segmento
       ,sta.des_est_tramite as estado
       ,cgr.cod_nup
       ,cgr.suc_cliente
    ,cgr.cuenta_cli
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malpe_pedt030 t030 on cgr.cod_nup = t030.penumper
join bi_corp_staging.malzx_alzxlsta sta on cgr.cod_est_tramite = sta.cod_est_tramite
where cgr.cod_est_tramite in ('14','17','26')
and t030.pecdgent= '0072' and cgr.cod_nup not in ('00199863','00199865', '03846180')
group by banca_cli,tpo_linea, t030.pesegcal, des_est_tramite, cgr.cod_nup, cgr.suc_cliente, cgr.cuenta_cli;