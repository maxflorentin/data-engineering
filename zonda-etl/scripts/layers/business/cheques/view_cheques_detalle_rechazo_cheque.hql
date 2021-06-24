CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_detalle_rechazo_cheque AS
select
       case tpo_linea
        when 'C' then 'carterizado'
        when 'E' then 'estandarizado'
        when 'X' then 'mayorista'
      end as cliente
        ,t030.pesegcal as segmento
        ,sum(imp_total_chq_rech) as monto_rech
        ,sum(cant_chq_rech)  as cant_cheques_rech
        ,dgr.mot_rech_cheque_tec
             ,cgr.cod_nup
             ,cgr.suc_cliente
             ,cgr.cuenta_cli
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malzx_alzxlsta sta on cgr.cod_est_tramite = sta.cod_est_tramite
join bi_corp_staging.malzx_alzxldgr dgr on cgr.nro_tramite = dgr.nro_tramite
join bi_corp_staging.malpe_pedt030 t030 on t030.penumper = cgr.cod_nup
where t030.pecdgent= '0072' and dgr.cod_rech_cheque not in ('00', '20')
  and cgr.cod_nup not in ('00199863', '00199865', '03846180')
group by sta.des_est_tramite, tpo_linea, dgr.mot_rech_cheque_tec, t030.pesegcal, cgr.cod_nup, cgr.suc_cliente, cgr.cuenta_cli;