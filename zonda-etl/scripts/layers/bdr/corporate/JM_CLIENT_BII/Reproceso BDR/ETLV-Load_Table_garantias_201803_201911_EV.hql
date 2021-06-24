set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.clientes_marcados as
select distinct num_persona as num_persona
from bi_corp_staging.view_clientes_en_mora
where cod_submarca_cliente IN ('20', '22') and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
UNION ALL
select distinct num_persona
from santander_business_risk_arda.marca_comite
where cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO') and data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_marca_comite', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
;

CREATE TEMPORARY TABLE bi_corp_bdr.clientes_desmarcados as
select distinct c.num_persona as num_persona
from bi_corp_staging.view_clientes_en_mora c
left join bi_corp_bdr.clientes_marcados m on c.num_persona = m.num_persona
where cod_submarca_cliente IN ('20', '22') and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
and m.num_persona is null
UNION ALL
select distinct c.num_persona
from santander_business_risk_arda.marca_comite c
left join bi_corp_bdr.clientes_marcados m on c.num_persona = m.num_persona
where cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO') and data_date_part = '{{ var.json.jm_client_bii.marca_comite_plwda}}'
and m.num_persona is null
;

CREATE TEMPORARY TABLE bi_corp_bdr.AVI_USERS as
select distinct num_persona
from santander_business_risk_arda.personas_valor_rgo rgo
where rgo.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_valor_rgo', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
and rgo.indicador ='AVI'
;


insert into table bi_corp_bdr.jm_client_bii
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}')
SELECT DISTINCT 
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}' AS G4093_FEOPERAC,
23100 AS G4093_S1EMP,
lpad(g.g4128_idnumcli, 9, '0') AS G4093_IDNUMCLI,
CASE WHEN c.cod_tip_persona IN ('F', 'J') THEN LPAD(bl23.cod_baremo_local, 5, '0')
     ELSE '99999' END AS G4093_TIP_PERS,
lpad(' ', 80, ' ') AS G4093_APNOMPER,
lpad(' ', 40, ' ') AS G4093_DATEXTO1,
lpad(' ', 40, ' ') AS G4093_DATEXTO2,
rpad(nvl(c.cod_tip_documento, ''), 40, ' ') AS G4093_IDENTPER,
lpad(nvl(' ', ' '), 40, ' ') AS G4093_CODIDPER,
lpad(0, 9, '0') AS G4093_CLIE_GLO,
lpad(' ', 40, ' ') AS G4093_IDSUCUR,
(CASE
  when aq.num_persona is not null then
    '00009'
  ELSE
    (case
      when msc.tipo_segmento_local_2 in ('C1','S','A','B','C','P1','P2') then
        '00002'
      when  msc.tipo_segmento_local_2 in ('EM','G1','GL','LA','LO','MU','OT')
        and (cast(nvl(juri.G5508_IMPFACTM,'0') as bigint) / nvl(cotiz.imp_cambio_fijo_vigente,cast('1' as double)) < cast('50000000' as int ))
        and (cast(nvl(juri.G5508_TDEUDACL,'0') as bigint) / nvl(cotiz.imp_cambio_fijo_vigente,cast('1' as double)) < cast('1000000' as int)) then
        '00002'
      else
        '00001'
     end)
  END) AS G4093_CARTER,
lpad(nvl(mb2.cod_baremo_global, 0) , 5, '0') AS G4093_ID_PAIS,
lpad(nvl(mb9.cod_baremo_global, 0), 5, '0') AS G4093_COD_SECT,
lpad(nvl(mb9.cod_baremo_local, 0), 5, '0') AS G4093_COD_SEC2,
case when g.g4128_idnumcli = '040708286' --Avalista MIGA
      then lpad(nvl(25, 0), 5, '0')
      else lpad(nvl(msc.cod_sector_contable, 0), 5, '0') 
end  AS G4093_COD_SEC3,
case when g.g4128_idnumcli = '040708286' --Avalista MIGA
      then lpad(nvl(25, 0), 5, '0')
      else lpad(nvl(mb24.cod_baremo_global, 0), 5, '0') 
end AS G4093_CLISEGM,
case when g.g4128_idnumcli = '040708286' --Avalista MIGA
      then lpad(nvl(25, 0), 5, '0')
      else lpad(nvl(mb24.cod_baremo_local, 0), 5, '0') 
end AS G4093_CLISEGL1,
rpad(nvl(c.cod_segmento, ''), 40, ' ') AS G4093_TIPSEGL1,
case when g.g4128_idnumcli = '040708286' --Avalista MIGA
      then lpad(nvl(25, 0), 5, '0')
      else lpad(nvl(msc.cod_segmento_local_2, 0), 5, '0') 
end AS G4093_CLISEGL2,
rpad(nvl(msc.tipo_segmento_local_2, ''), 40, ' ') AS G4093_TIPSEGL2,
nvl(p.fec_ingreso,'0001-01-01') AS G4093_FCHINI,
'9999-12-31' AS G4093_FCHFIN,
nvl(to_date(p.fec_ultima_modificacion),'0001-01-01') AS G4093_FECULTMO,
lpad(nvl(cast(a.sector_interno_1 as int), 0), 5, '0') AS G4093_INDUSTRY,
lpad(0, 5, '0') AS G4093_EXCLCLI,
CASE WHEN c.cod_submarca_subjetiva ='20' OR c.cod_submarca_subjetiva ='22' THEN 'S' ELSE 'N' END AS G4093_INDBCART,
CASE WHEN c.cod_tip_persona LIKE 'F' THEN nvl(c.fec_nacimiento,'0001-01-01') ELSE nvl(p.inicio_actividad,'0001-01-01') END AS G4093_FECNACIM,
'00032' AS G4093_PAISNEG,
lpad(' ', 40, ' ') AS G4093_CDPOSTAL,
case when g.g4128_idnumcli = '040708286'
            then lpad(5, 5, '0')
            else lpad(0, 5, '0') 
        end AS G4093_TTO_ESPE,
CASE when rgo.valido = '1' then  '00000' when rgo.valido in ( '2', 'A') then '00001' when rgo.valido in ('4', '5', 'T') then '00002' when rgo.valido in ('3', 'V', 'P') then '00003' else '99999' end As G4093_GRA_VINC,
CASE WHEN m.num_persona is not null and mc.num_persona is not null and utp.num_persona is not null and d.num_persona is null THEN '00003'
     WHEN m.num_persona is not null and mc.num_persona is null and utp.num_persona is not null and d.num_persona is null THEN '00001'
     ELSE '99999' END AS G4093_UTP_CLI,
CASE WHEN m.num_persona is not null and d.num_persona is null THEN utp.max_fec_inicio_vigencia ELSE '0001-01-01' END AS G4093_FINIUTCL,
CASE WHEN d.num_persona is not null THEN to_date(from_unixtime(UNIX_TIMESTAMP(concat(substring(c.data_date_part, 1, 6), '01'),"yyyyMMdd"))) ELSE '9999-12-31' END AS G4093_FFINUTCL
FROM bi_corp_bdr.jm_interv_cto g
LEFT JOIN bi_corp_bdr.jm_client_bii cb
      ON cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
     AND g.g4128_idnumcli = cb.g4093_idnumcli
LEFT JOIN santander_business_risk_arda.clientes c
      ON g.g4128_idnumcli = lpad(c.num_persona,9,'0')
     AND c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_clientes', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
LEFT JOIN santander_business_risk_arda.personas p
      ON lpad(p.num_persona,9,'0') = g.g4128_idnumcli
     AND p.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
LEFT JOIN bi_corp_bdr.marca_utp_clientes utp
      ON lpad(utp.num_persona,9,'0') = g.g4128_idnumcli
     AND utp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
LEFT JOIN santander_business_risk_arda.marca_comite mc
      ON lpad(mc.num_persona,9,'0') = g.g4128_idnumcli
     AND mc.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_marca_comite', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
     AND mc.cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO')
LEFT JOIN bi_corp_bdr.clientes_marcados m
      ON lpad(m.num_persona,9,'0') = g.g4128_idnumcli
LEFT JOIN bi_corp_bdr.clientes_desmarcados d
      ON lpad(d.num_persona,9,'0') = g.g4128_idnumcli
LEFT JOIN bi_corp_bdr.baremos_local bl23
      ON bl23.cod_baremo_alfanumerico_local = c.cod_tip_persona
     AND bl23.cod_negocio_local = '23'
LEFT JOIN bi_corp_bdr.baremos_local bl2
      ON bl2.cod_baremo_local = p.residencia_cliente
     AND bl2.cod_negocio_local = '2'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb2
      ON mb2.cod_baremo_local = bl2.cod_baremo_local
     AND mb2.cod_negocio = 2
LEFT JOIN santander_business_risk_arda.personas_valor_rgo rgo
      ON lpad(rgo.num_persona,9,'0') = g.g4128_idnumcli
     AND rgo.indicador in ('AVI', 'AYV')
     AND rgo.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_valor_rgo', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
LEFT JOIN bi_corp_bdr.baremos_local bl9
      ON bl9.cod_baremo_alfanumerico_local = cast(cast(c.cod_afip as int) as string)
     AND bl9.cod_negocio_local = '9'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb9
      ON mb9.cod_baremo_local = bl9.cod_baremo_local
     AND mb9.cod_negocio = 9
LEFT JOIN bi_corp_bdr.baremos_local bl24
      ON bl24.cod_baremo_alfanumerico_local = c.cod_segmento
     AND bl24.cod_negocio_local = '24'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24
      ON mb24.cod_baremo_local = bl24.cod_baremo_local
     AND mb24.cod_negocio = 24
LEFT JOIN bi_corp_bdr.map_sector_contable msc
      ON msc.tipo_segmento_local_1 = bl24.cod_baremo_alfanumerico_local
LEFT JOIN bi_corp_bdr.AVI_USERS AVIU
      ON lpad(AVIU.num_persona,9,'0') = g.g4128_idnumcli
LEFT JOIN 
      ( 
        select trim(mkg.nup) as nup
                ,mkg.shortname
          from bi_corp_bdr.perim_mdr_contraparte mkg
          where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'               
      ) k
      ON lpad(trim(k.nup),9,'0') = g.g4128_idnumcli
LEFT JOIN bi_corp_staging.aqua_clientes_asoc_geconomicos a
      ON trim(a.unidad_operativa) = trim(k.shortname)
     AND a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
LEFT JOIN     
    ( 
      select distinct num_persona 
      from bi_corp_bdr.perim_rating_aqua 
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
    ) aq
      ON lpad(aq.num_persona,9,'0') = g.g4128_idnumcli
LEFT JOIN santander_business_risk_arda.rating_sge sge_cli
      ON lpad(sge_cli.num_persona,9,'0') = g.g4128_idnumcli
     AND sge_cli.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_rating_sge', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
left join bi_corp_bdr.JM_CLIEN_JURI juri on juri.g5508_idnumcli = lpad(trim(c.num_persona), 9, '0') and juri.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
left join (SELECT cod_divisa, data_date_part, cast((imp_cambio_fijo/100)as double) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
    	     FROM santander_business_risk_arda.cotizacion
    	    WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
    		  and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
    	      and cod_divisa = 'EUR'
    		  and fec_cambio = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}') cotiz on (cod_divisa ='EUR')
WHERE g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_EV_AD_CB') }}'
  and g.g4128_tipintev = '00002'
  and g.g4128_tipintv2 = '00005'
  and not ( AVIU.num_persona is not null and rgo.indicador= 'AYV' )
  and cb.g4093_idnumcli is null
;