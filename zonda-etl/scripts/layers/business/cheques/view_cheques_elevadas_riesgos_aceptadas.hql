CREATE VIEW IF NOT EXISTS  bi_corp_business.view_cheques_elevadas_riesgos_aceptadas AS
select count(*) as cant_ope
      ,sum(imp_total_chq_ok) as imp_tot_chq_ok
      ,sum(cant_chq_ok) as cant_chq_ok
      ,case tpo_linea
       when 'C' then 'carterizado'
       when 'E' then 'estandarizado'
       when 'X' then 'mayorista'
       end as cliente
      ,t030.pesegcal as segmento
      ,case cod_est_tramite
       when '22' then 'aceptada'
       when '23' then 'aceptada'
       when '24' then 'aceptada'
       end as estado
from bi_corp_staging.malzx_alzxucgr ctz
join bi_corp_staging.malpe_pedt030 t030
on   ctz.cod_nup = t030.penumper
where cod_est_tramite in ('22', '23', '24') and ctz.marca_operacion_esp = 'S'
and t030.pecdgent= '0072'
and ctz.nro_tramite in (select a.nro_tramite from bi_corp_staging.malzx_alzxucgr a where a.cod_est_tramite = '11')
and ctz.cod_nup not in ('00199863', '00199865', '03846180')
group by banca_cli, tpo_linea, t030.pesegcal, cod_est_tramite;