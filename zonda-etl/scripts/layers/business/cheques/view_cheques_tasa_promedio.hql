CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_tasa_promedio AS
select count(*) as cant_ope
     ,avg(tasa_aplicada) as prom_tasa_apli_acre
    ,case tpo_linea
      when 'C' then 'carterizado'
      when 'E' then 'estandarizado'
      when 'X' then 'mayorista'
    end as cliente
     ,t030.pesegcal as segmento
from bi_corp_staging.malzx_alzxucgr cgr
join bi_corp_staging.malpe_pedt030 t030 on cgr.cod_nup = t030.penumper
where cod_est_tramite in ('22', '23', '24') and t030.pecdgent= '0072'
and cgr.cod_nup not in ('00199863', '00199865', '03846180')
group by banca_cli, tpo_linea, t030.pesegcal;