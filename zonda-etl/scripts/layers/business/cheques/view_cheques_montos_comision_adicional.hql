CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_montos_comision_adicional AS
select
        sum(monto_comision_adic) as tot_com_adi
       ,fec_tramite
         ,case tpo_linea
          when 'c' then 'carterizado'
          when 'e' then 'estandarizado'
          when 'x' then 'mayorista'
          end as cliente
       ,t030.pesegcal
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malpe_pedt030 t030 on cgr.cod_nup = t030.penumper
where cod_est_tramite in ('22', '23', '24')
and t030.pecdgent= '0072' and cgr.cod_nup not in ('00199863', '00199865', '03846180')
group by fec_tramite, banca_cli, tpo_linea, t030.pesegcal;