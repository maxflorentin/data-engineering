CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_detalle_rechazo_tramite AS
select
        case tpo_linea
         when 'C' then 'carterizado'
         when 'E' then 'estandarizado'
         when 'X' then 'mayorista'
       end as cliente
         ,t030.pesegcal as segmento
         ,sum(imp_total_chq)      as monto_rech
         ,sum(cant_chq)  as cant_cheques_rech
         ,sta.des_est_tramite
         ,crh.des_rechazo_ofc
         ,cgr.cod_nup
         ,cgr.suc_cliente
         ,cgr.cuenta_cli
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malzx_alzxlsta sta on cgr.cod_est_tramite = sta.cod_est_tramite
join bi_corp_staging.malzx_alzxlcrh crh on cgr.cod_motivo_rechazo = crh.cod_rechazo
join bi_corp_staging.malpe_pedt030 t030 on t030.penumper = cgr.cod_nup
where t030.pecdgent= '0072' and cgr.cod_est_tramite = '06' and crh.tpo_rechazo= 'trm' and cgr.cod_nup not in ('00199863', '00199865','03846180')
group by sta.des_est_tramite, banca_cli, tpo_linea, cgr.cod_nup, crh.des_rechazo_ofc, t030.pesegcal, cgr.cod_nup, cgr.suc_cliente, cgr.cuenta_cli;
