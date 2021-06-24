CREATE VIEW IF NOT EXISTS bi_corp_business.view_cheques_cantidad_operaciones_acreditadas AS
select count(*) as cant_ope
      ,sum(imp_total_chq) as monto_ope
      ,sum(cant_chq) as cant_cheques_ope
      ,fec_tramite as fecha_alta
      ,t030.pesegcal as segmento
      ,case tpo_linea
        when 'C' then 'carterizado'
        when 'E' then 'estandarizado'
        when 'X' then 'mayorista'
      end as cliente
      ,case cod_canal
        when '22' then '+che'
        when '04' then 'obe'
        when '07' then 'obcm'
      end as canal
      ,sta.des_est_tramite as estado
from bi_corp_staging.malzx_alzxucgr cgr
 join bi_corp_staging.malpe_pedt030 t030 on cgr.cod_nup = t030.penumper
 join bi_corp_staging.malzx_alzxlsta sta on cgr.cod_est_tramite = sta.cod_est_tramite
 where t030.pecdgent= '0072' and cgr.cod_est_tramite in ('22', '23', '24')
   and cgr.cod_nup not in ('00199863', '00199865','03846180')
 group by fec_tramite, tpo_linea, cod_canal, des_est_tramite, t030.pesegcal
 order by fec_tramite desc;