CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_elevadas_riesgos_rechazadas AS
select count(*) as cant_ope
      ,sum(imp_total_chq_rech) as imp_tot_chq_rech
      ,sum(cant_chq_rech) as cant_chq_rech
      ,case tpo_linea
       when 'C' then 'carterizado'
       when 'E' then 'estandarizado'
       when 'X' then 'mayorista'
       end as cliente
      ,t030.pesegcal as segmento
      ,ctz.cod_nup
      ,ctz.suc_cliente
      ,ctz.cuenta_cli
from bi_corp_staging.malzx_alzxucgr ctz
join bi_corp_staging.malpe_pedt030 t030 on ctz.cod_nup = t030.penumper
where t030.pecdgent= '0072' and cod_est_tramite in ( '06', '14', '16', '17', '25', '26') and ctz.nro_tramite in (select a.nro_tramite from bi_corp_staging.malzx_alzxucgr a where a.cod_est_tramite = '11') and ctz.cod_nup not in ('00199863', '00199865', '03846180') and ctz.marca_operacion_esp = 'S'
group by banca_cli, tpo_linea, t030.pesegcal, ctz.cod_nup, ctz.suc_cliente, ctz.cuenta_cli;