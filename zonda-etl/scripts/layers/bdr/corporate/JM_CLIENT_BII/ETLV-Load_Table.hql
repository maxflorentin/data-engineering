set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
set hive.strict.checks.cartesian.product=false;

--Táctico Corresponsales
CREATE TEMPORARY TABLE bi_corp_bdr.tactico_corresponsales as
select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AS G4093_FEOPERAC
        ,'23100' AS G4093_S1EMP
        ,lpad(crp.nup , 9, '0') AS G4093_IDNUMCLI --Hay que ir al tabla de bi_corp_bdr.normalizacion_id_clientes
        ,lpad('2', 5, '0')  AS G4093_TIP_PERS -- Juridica
        ,lpad(' ', 80, ' ') AS G4093_APNOMPER
        ,lpad(' ', 40, ' ') AS G4093_DATEXTO1
        ,lpad(' ', 40, ' ') AS G4093_DATEXTO2
        ,rpad(' ', 40, ' ') AS G4093_IDENTPER
        ,lpad(' ', 40, ' ') AS G4093_CODIDPER
        ,lpad('0', 9, '0') AS G4093_CLIE_GLO
        ,lpad(' ', 40, ' ') AS G4093_IDSUCUR
		    ,cast(null as string) AS G4093_CARTER
        ,lpad(crp.pais, 5, '0') AS G4093_ID_PAIS --Código del pais
        ,lpad('26', 5, '0') AS G4093_COD_SECT --Valor por defecto: 26-Intermediación Financiera
        ,lpad('0', 5, '0') AS G4093_COD_SEC2
        ,lpad(crp.sector_contable , 5, '0') AS G4093_COD_SEC3
        ,case
          when crp.nup = '00200306' then
            '00008'
          else
            lpad(crp.segmento_cliente , 5, '0')
        end G4093_CLISEGM
        ,lpad('0', 5, '0') AS G4093_CLISEGL1
        ,rpad(crp.tipo_segmento_local_1, 40, ' ') AS G4093_TIPSEGL1
        ,lpad('0', 5, '0') AS G4093_CLISEGL2
        ,rpad(crp.tipo_segmento_local_2, 40, ' ') AS G4093_TIPSEGL2
        ,'0001-01-01' AS G4093_FCHINI
        ,'9999-12-31' AS G4093_FCHFIN
        ,'9999-12-31' AS G4093_FECULTMO
        ,lpad('0', 5, '0') AS G4093_INDUSTRY
        ,lpad('0', 5, '0') AS G4093_EXCLCLI
        ,' ' AS G4093_INDBCART
        ,'0001-01-01' AS G4093_FECNACIM
        ,'00032' AS G4093_PAISNEG
        ,lpad(' ', 40, ' ') AS G4093_CDPOSTAL
        ,case when lpad(crp.nup , 9, '0') = '040708286' then lpad('5', 5, '0') else lpad('0', 5, '0') end AS G4093_TTO_ESPE
        ,'99999'  As G4093_GRA_VINC
        ,'99999'  AS G4093_UTP_CLI
        ,'0001-01-01' AS G4093_FINIUTCL
        ,'9999-12-31' AS G4093_FFINUTCL
from
    (
        select distinct crp.nup
                ,crp.banco
                ,crp.pais
                ,crp.sector_contable
                ,crp.segmento_cliente
                ,crp.tipo_segmento_local_1
                ,crp.tipo_segmento_local_2
        from bi_corp_staging.corresponsales crp
       where crp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_corresponsales', dag_id='BDR_LOAD_Tables-Monthly') }}'
    ) crp
;

insert overwrite table bi_corp_bdr.jm_client_bii
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select  G4093_FEOPERAC
        ,G4093_S1EMP
        ,G4093_IDNUMCLI
        ,G4093_TIP_PERS
        ,G4093_APNOMPER
        ,G4093_DATEXTO1
        ,G4093_DATEXTO2
        ,G4093_IDENTPER
        ,G4093_CODIDPER
        ,G4093_CLIE_GLO
        ,G4093_IDSUCUR
        ,G4093_CARTER
        ,G4093_ID_PAIS
        ,G4093_COD_SECT
        ,G4093_COD_SEC2
        ,G4093_COD_SEC3
        ,G4093_CLISEGM
        ,G4093_CLISEGL1
        ,G4093_TIPSEGL1
        ,G4093_CLISEGL2
        ,G4093_TIPSEGL2
        ,G4093_FCHINI
        ,G4093_FCHFIN
        ,G4093_FECULTMO
        ,G4093_INDUSTRY
        ,G4093_EXCLCLI
        ,G4093_INDBCART
        ,G4093_FECNACIM
        ,G4093_PAISNEG
        ,G4093_CDPOSTAL
        ,G4093_TTO_ESPE
        ,G4093_GRA_VINC
        ,G4093_UTP_CLI
        ,G4093_FINIUTCL
        ,G4093_FFINUTCL
from bi_corp_bdr.tactico_corresponsales
;


--MMFF Clientes BIS

with origen_rtra as (select distinct t.*
                       from (select cpty,
                                    industry,
                                    dealstamp
                               from bi_corp_staging.rtra_regulatorio
                              where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
                              union all
                             select cpty,
                                    industry,
                                    dealstamp
                               from bi_corp_staging.rtra_economico
                              where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
                            ) t
                     )
insert into table bi_corp_bdr.jm_client_bii
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
 select  distinct
       --'2020-09-30'  as G5519_FEOPERAC,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'  as G5519_FEOPERAC,
        '23100' as G5519_S1EMP,
        lpad(nvl(mdr.nup,'0'), 9, '0') as G4093_IDNUMCLI,
        case
          when trim(cl.cod_tip_persona) = 'F' then
            '00001'
          when trim(cl.cod_tip_persona) = 'J' then
            '00002'
          when trim(cl.cod_tip_persona) is null or trim(cl.cod_tip_persona)  = '' then
            '00000'
          else
            '99999'
        end as G4093_TIP_PERS,
        --rpad(cl.nombre_persona, 80, ' ') as G4093_APNOMPER,
        rpad(' ', 80, ' ') as G4093_APNOMPER,
        lpad(' ', 40, ' ') AS G4093_DATEXTO1,
        lpad(' ', 40, ' ') AS G4093_DATEXTO2,
        rpad(nvl(' ', ''), 40, ' ') AS G4093_IDENTPER,
        lpad(nvl(' ', ' '), 40, ' ') AS G4093_CODIDPER,
        lpad('0', 9, '0') AS G4093_CLIE_GLO,
        lpad(' ', 40, ' ') AS G4093_IDSUCUR,
		    null AS G4093_CARTER,
        --'00000' as G4093_CARTER,
        lpad('0' , 5, '0') AS G4093_ID_PAIS,
        case
          when rtra.cpty = 'BCEA' then
            '00026'
          else
            lpad(nvl(mb9.cod_baremo_global, '0'), 5, '0')
        end G4093_COD_SECT, --agrego codigo 00026
        --lpad(nvl(mb9.cod_baremo_global, '0'), 5, '0') AS G4093_COD_SECT,
		lpad(nvl(mb9.cod_baremo_local, '0'), 5, '0') AS G4093_COD_SEC2,
		lpad(nvl(cast(msc.cod_sector_contable as string), '0'), 5,'0') AS G4093_COD_SEC3,
		case
		  when rtra.cpty = 'BCEA' then
		    '00008'
		  else
		    lpad(nvl(mb24.cod_baremo_global, '0'), 5, '0')
        end G4093_CLISEGM,
		--lpad(nvl(mb24.cod_baremo_global, '0'), 5, '0') AS G4093_CLISEGM,
		lpad(nvl(mb24.cod_baremo_local, '0'), 5, '0') AS G4093_CLISEGL1,
		rpad(nvl(cl.cod_segmento, ''), 40, ' ') as G4093_TIPSEGL1,
		lpad(nvl(cast(msc.cod_segmento_local_2 as string), '0'), 5, '0') AS G4093_CLISEGL2,
		rpad(nvl(cast(cl.cod_segmento_calculado as string), ''), 40, ' ') as G4093_TIPSEGL2,
		nvl(cl.fec_ingreso,'0001-01-01') as G4093_FCHINI,
		case
		  when cl.num_persona is not null then
		    '9999-12-31'
		  else
		    '0001-01-01'
		end G4093_FCHFIN,
		'0001-01-01' AS G4093_FECULTMO,
		lpad(nvl(rtra.industry , '0'), 5, '0') as G4093_INDUSTRY,
        lpad('0', 5, '0') AS G4093_EXCLCLI,
        ' ' AS G4093_INDBCART,
        '0001-01-01' AS G4093_FECNACIM,
        '00000' AS G4093_PAISNEG,
        lpad(' ', 40, ' ') AS G4093_CDPOSTAL,
		case when lpad(nvl(mdr.nup,'0'), 9, '0') = '040708286' then lpad('5', 5, '0') else lpad('0', 5, '0') end AS G4093_TTO_ESPE,
        '00000' AS G4093_GRA_VINC,
        '00000' AS G4093_UTP_CLI,
        '0001-01-01' AS G4093_FINIUTCL,
        '9999-12-31' AS G4093_FFINUTCL
   from origen_rtra rtra
  inner join bi_corp_staging.mmff_tactico_especie tac
     on tac.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly') }}'
    and rtra.dealstamp = concat(tac.especie,'-I')
  inner join 
    ( 
      select trim(mkg.nup) as nup
              ,mkg.shortname
        from bi_corp_bdr.perim_mdr_contraparte mkg
        where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'               
    ) mdr     
    on trim(mdr.shortname) = trim(rtra.cpty)
   left join santander_business_risk_arda.clientes cl
     on mdr.nup = cl.num_persona
    and cl.condicion_del_cliente = 'CLI'
    and cl.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_clientes', dag_id='BDR_LOAD_Tables-Monthly') }}'
   left join bi_corp_bdr.baremos_local bl24
     on bl24.cod_baremo_alfanumerico_local = cl.cod_segmento
    and bl24.cod_negocio_local = '24'
   left join bi_corp_bdr.baremos_local bl9
     on bl9.cod_baremo_alfanumerico_local = cast(cast(cl.cod_afip as int) as string)
    and bl9.cod_negocio_local = '9'
    -- join para calculo G4093_COD_SECT, G4093_COD_SEC2
   left join bi_corp_bdr.map_baremos_global_local mb9
     on mb9.cod_baremo_local = bl9.cod_baremo_local
    and mb9.cod_negocio = 9
   --join para el calculo de G4093_COD_SEC3,G4093_CLISEGL2
   left join bi_corp_bdr.map_sector_contable msc
     on msc.tipo_segmento_local_1 = bl24.cod_baremo_alfanumerico_local
   --join para calculo de G4093_CLISEGM, G4093_CLISEGL1
   LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24
     on mb24.cod_baremo_local = bl24.cod_baremo_local
    and mb24.cod_negocio = 24
 left JOIN santander_business_risk_arda.rating_sge sge_cli 
    ON sge_cli.num_persona = mdr.nup 
    and sge_cli.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_rating_sge', dag_id='BDR_LOAD_Tables-Monthly') }}'
 --left join bi_corp_bdr.client_bii cb on trim(lpad(nvl(mdr.alias_nup,'0'), 9, '0')) = trim(cb.G4093_IDNUMCLI) and cb.partition_date ='2020-09'
 left join
    (
      select *
      from bi_corp_bdr.tactico_corresponsales crr
    ) crp
        on trim(lpad(nvl(mdr.nup,'0'), 9, '0')) = trim(crp.G4093_IDNUMCLI)
  where crp.G4093_IDNUMCLI is null
;

CREATE TEMPORARY TABLE bi_corp_bdr.clientes_marcados as
select distinct num_persona as num_persona
from bi_corp_staging.view_clientes_en_mora
where cod_submarca_cliente IN ('20', '22') and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
UNION ALL
select distinct num_persona
from santander_business_risk_arda.marca_comite
where cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO') and data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_marca_comite', dag_id='BDR_LOAD_Tables-Monthly') }}'
;

CREATE TEMPORARY TABLE bi_corp_bdr.clientes_desmarcados as
select distinct c.num_persona as num_persona
from bi_corp_staging.view_clientes_en_mora c
left join bi_corp_bdr.clientes_marcados m on c.num_persona = m.num_persona
where cod_submarca_cliente IN ('20', '22') and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
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
where rgo.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_valor_rgo', dag_id='BDR_LOAD_Tables-Monthly') }}'
and rgo.indicador ='AVI'
;

insert into table bi_corp_bdr.jm_client_bii
partition(partition_date)

SELECT DISTINCT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AS G4093_FEOPERAC,
23100 AS G4093_S1EMP,
lpad(c.num_persona, 9, '0') AS G4093_IDNUMCLI,
CASE WHEN c.cod_tip_persona IN ('F', 'J') THEN LPAD(bl23.cod_baremo_local, 5, '0')
     ELSE '99999' END AS G4093_TIP_PERS,
lpad(' ', 80, ' ') AS G4093_APNOMPER,
lpad(' ', 40, ' ') AS G4093_DATEXTO1,
lpad(' ', 40, ' ') AS G4093_DATEXTO2,
rpad(nvl(c.cod_tip_documento, ''), 40, ' ') AS G4093_IDENTPER,
lpad(nvl(' ', ' '), 40, ' ') AS G4093_CODIDPER,
lpad(0, 9, '0') AS G4093_CLIE_GLO,
lpad(' ', 40, ' ') AS G4093_IDSUCUR,
null AS G4093_CARTER,
lpad(nvl(mb2.cod_baremo_global, 0) , 5, '0') AS G4093_ID_PAIS,
lpad(nvl(mb9.cod_baremo_global, 0), 5, '0') AS G4093_COD_SECT,
lpad(nvl(mb9.cod_baremo_local, 0), 5, '0') AS G4093_COD_SEC2,
lpad(nvl(msc.cod_sector_contable, 0), 5, '0') AS G4093_COD_SEC3,
lpad(nvl(mb24.cod_baremo_global, 0), 5, '0') AS G4093_CLISEGM,
lpad(nvl(mb24.cod_baremo_local, 0), 5, '0') AS G4093_CLISEGL1,
rpad(nvl(c.cod_segmento, ''), 40, ' ') AS G4093_TIPSEGL1,
lpad(nvl(msc.cod_segmento_local_2, 0), 5, '0') AS G4093_CLISEGL2,
rpad(nvl(msc.tipo_segmento_local_2, ''), 40, ' ') AS G4093_TIPSEGL2,
nvl(p.fec_ingreso,'0001-01-01') AS G4093_FCHINI,
'9999-12-31' AS G4093_FCHFIN,
nvl(to_date(p.fec_ultima_modificacion),'0001-01-01') AS G4093_FECULTMO,
lpad(nvl(cast(a.sector_interno_1 as int), 0), 5, '0') AS G4093_INDUSTRY,
lpad(0, 5, '0') AS G4093_EXCLCLI,
CASE WHEN c.cod_submarca_subjetiva ='20' OR c.cod_submarca_subjetiva ='22' THEN 'S' ELSE 'N' END AS G4093_INDBCART,
nvl((CASE WHEN c.cod_tip_persona LIKE 'F' THEN c.fec_nacimiento ELSE p.inicio_actividad END),'0001-01-01') AS G4093_FECNACIM,
'00032' AS G4093_PAISNEG,
lpad(' ', 40, ' ') AS G4093_CDPOSTAL,
lpad(0, 5, '0') AS G4093_TTO_ESPE,
CASE when rgo.valido = '1' then  '00000' when rgo.valido in ( '2', 'A') then '00001' when rgo.valido in ('4', '5', 'T') then '00002' when rgo.valido in ('3', 'V', 'P') then '00003' else '99999' end As G4093_GRA_VINC,
CASE WHEN m.num_persona is not null and mc.num_persona is not null and utp.num_persona is not null and d.num_persona is null THEN '00003'
     WHEN m.num_persona is not null and mc.num_persona is null and utp.num_persona is not null and d.num_persona is null THEN '00001'
     ELSE '99999' END AS G4093_UTP_CLI,
CASE WHEN m.num_persona is not null and d.num_persona is null THEN utp.max_fec_inicio_vigencia ELSE '0001-01-01' END AS G4093_FINIUTCL,
CASE WHEN d.num_persona is not null THEN to_date(from_unixtime(UNIX_TIMESTAMP(concat(substring(c.data_date_part, 1, 6), '01'),"yyyyMMdd"))) ELSE '9999-12-31' END AS G4093_FFINUTCL,
'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}' as partition_date
FROM
(
  select c.*
  from
    (
      select *
      from santander_business_risk_arda.clientes c
      where c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_clientes', dag_id='BDR_LOAD_Tables-Monthly') }}'
        and c.condicion_del_cliente = 'CLI'
    ) c
      left join
    (
      select *
      from bi_corp_bdr.jm_client_bii cbi
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
    ) cbi
        on lpad(trim(c.num_persona), 9, '0') = trim(cbi.G4093_IDNUMCLI)
  where cbi.G4093_IDNUMCLI is null
 ) c
INNER JOIN santander_business_risk_arda.personas p ON p.num_persona = c.num_persona AND p.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.marca_utp_clientes utp on utp.num_persona = c.num_persona and utp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN santander_business_risk_arda.marca_comite mc on mc.num_persona = c.num_persona and mc.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_marca_comite', dag_id='BDR_LOAD_Tables-Monthly') }}'
and mc.cod_marca_subjetiva IN ('PR', 'DE', 'MO', 'CO')
LEFT JOIN bi_corp_bdr.clientes_marcados m ON c.num_persona = m.num_persona
LEFT JOIN bi_corp_bdr.clientes_desmarcados d ON c.num_persona = d.num_persona
LEFT JOIN bi_corp_bdr.baremos_local bl23 on bl23.cod_baremo_alfanumerico_local = c.cod_tip_persona and bl23.cod_negocio_local = '23'
LEFT JOIN bi_corp_bdr.baremos_local bl2 on bl2.cod_baremo_local = p.residencia_cliente and bl2.cod_negocio_local = '2'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb2 ON mb2.cod_baremo_local = bl2.cod_baremo_local and mb2.cod_negocio = 2
LEFT JOIN santander_business_risk_arda.personas_valor_rgo rgo on rgo.num_persona = p.num_persona and rgo.indicador in ('AVI', 'AYV') and rgo.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_personas_valor_rgo', dag_id='BDR_LOAD_Tables-Monthly') }}'
LEFT JOIN bi_corp_bdr.baremos_local bl9 on bl9.cod_baremo_alfanumerico_local = cast(cast(c.cod_afip as int) as string) and bl9.cod_negocio_local = '9'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb9 on mb9.cod_baremo_local = bl9.cod_baremo_local and mb9.cod_negocio = 9
LEFT JOIN bi_corp_bdr.baremos_local bl24 on bl24.cod_baremo_alfanumerico_local = c.cod_segmento and bl24.cod_negocio_local = '24'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24 on mb24.cod_baremo_local = bl24.cod_baremo_local and mb24.cod_negocio = 24
LEFT JOIN bi_corp_bdr.map_sector_contable msc on msc.tipo_segmento_local_1 = bl24.cod_baremo_alfanumerico_local
LEFT JOIN bi_corp_bdr.AVI_USERS AVIU on AVIU.num_persona = c.num_persona
LEFT JOIN 
      ( 
        select trim(mkg.nup) as nup
                ,mkg.shortname
          from bi_corp_bdr.perim_mdr_contraparte mkg
          where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'               
      ) k   on trim(k.nup) = c.num_persona
LEFT JOIN bi_corp_staging.aqua_clientes_asoc_geconomicos a 
    on trim(a.unidad_operativa) = trim(k.shortname) 
      and a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
LEFT JOIN santander_business_risk_arda.rating_sge sge_cli 
    ON sge_cli.num_persona=c.num_persona 
    and sge_cli.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_rating_sge', dag_id='BDR_LOAD_Tables-Monthly') }}'
WHERE not ( AVIU.num_persona is not null and rgo.indicador= 'AYV' );


insert into table bi_corp_bdr.jm_client_bii
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT distinct
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' AS G4093_FEOPERAC,
23100 AS G4093_S1EMP,
lpad(c.id_cte_bdr , 9, '0') AS G4093_IDNUMCLI,
lpad(0, 5, 0)  AS G4093_TIP_PERS,
lpad(' ', 80, ' ') AS G4093_APNOMPER,
lpad(' ', 40, ' ') AS G4093_DATEXTO1,
lpad(' ', 40, ' ') AS G4093_DATEXTO2,
rpad(' ', 40, ' ') AS G4093_IDENTPER,
lpad(' ', 40, ' ') AS G4093_CODIDPER,
lpad(0, 9, 0) AS G4093_CLIE_GLO,
lpad(' ', 40, ' ') AS G4093_IDSUCUR,
lpad(0, 5, 0)  AS G4093_CARTER,
lpad(0, 5, 0) AS G4093_ID_PAIS,
lpad(0, 5, 0) AS G4093_COD_SECT,
lpad(0, 5, 0) AS G4093_COD_SEC2,
lpad(0, 5, 0) AS G4093_COD_SEC3,
lpad(0, 5, 0) AS G4093_CLISEGM,
lpad(0, 5, 0) AS G4093_CLISEGL1,
rpad(' ', 40, ' ') AS G4093_TIPSEGL1,
lpad(0, 5, 0) AS G4093_CLISEGL2,
rpad(' ', 40, ' ') AS G4093_TIPSEGL2,
'0001-01-01' AS G4093_FCHINI,
'9999-12-31' AS G4093_FCHFIN,
'9999-12-31' AS G4093_FECULTMO,
lpad(0, 5, 0) AS G4093_INDUSTRY,
lpad(0, 5, 0) AS G4093_EXCLCLI,
' ' AS G4093_INDBCART,
'0001-01-01' AS G4093_FECNACIM,
'00032' AS G4093_PAISNEG,
lpad(' ', 40, ' ') AS G4093_CDPOSTAL,
lpad(0, 5, 0) AS G4093_TTO_ESPE,
'99999'  As G4093_GRA_VINC,
'99999'  AS G4093_UTP_CLI,
'0001-01-01' AS G4093_FINIUTCL,
'9999-12-31' AS G4093_FFINUTCL
FROM bi_corp_bdr.normalizacion_id_clientes c
;
