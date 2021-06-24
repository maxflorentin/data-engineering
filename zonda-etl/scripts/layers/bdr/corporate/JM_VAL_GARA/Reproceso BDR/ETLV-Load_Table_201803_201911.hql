set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.cotizaciones AS
SELECT cod_divisa, data_date_part, (imp_cambio_fijo/100) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
FROM santander_business_risk_arda.cotizacion
WHERE data_date_part =  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
AND ind_divisa = 'D'
AND ind_cotizacion = 'S'
AND cod_segmento = ''
AND concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_val_gara
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
select distinct 
       G4134_FEOPERAC,
       G4134_S1EMP,
       G4134_BIENGAR1,
       case
         when G4134_FECHVALR < add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}',-12) then
           '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
         ELSE
           G4134_FECHVALR
       END AS G4134_FECHVALR,    --nvl(to_date(cast(add_months(ms.fec_dato ,2) as string)),'9999-12-31') as E9954_FECCADUC
       G4134_NOMENTI,
       G4134_IMGARANT,
       G4134_FECULTMO,
       G4134_TIPVALN,
       G4134_TIP_GARA,
       G4134_CODDIV
from (
SELECT DISTINCT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS G4134_FEOPERAC,
'23100' AS G4134_S1EMP,
lpad(m.gttcmae_mae_num_garantia, 9, '0') AS G4134_BIENGAR1,
CASE
  WHEN v.gttcvab_vab_num_bien is not null THEN
    t.gttctas_tas_fec_tasacion
  ELSE
    b.gttcbie_bie_fec_alta
END AS G4134_FECHVALR,
lpad('', 40, ' ') AS G4134_NOMENTI,
--lpad((CASE WHEN v.gttcvab_vab_num_bien is not null THEN v.gttcvab_vab_imp_total * nvl(tmp.imp_cambio_fijo_vigente , 1) ELSE m.gttcmae_mae_imp_limite * nvl(tmp.imp_cambio_fijo_vigente, 1) END), 17,'0') AS G4134_IMGARANT,
lpad(regexp_replace(format_number(cast(regexp_replace(nvl((CASE WHEN v.gttcvab_vab_num_bien is not null THEN v.gttcvab_vab_imp_total * nvl(tmp.imp_cambio_fijo_vigente , 1) ELSE m.gttcmae_mae_imp_limite * nvl(tmp.imp_cambio_fijo_vigente, 1) END),0) ,"\\,",".") as double), 2),"\\,|\\.|\\+",""),17,"0") AS G4134_IMGARANT,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' AS G4134_FECULTMO,
lpad('1', 5, '0') AS G4134_TIPVALN,
lpad(c.cod_des_textored, 5, '0') AS G4134_TIP_GARA,
'ARS' AS G4134_CODDIV
FROM bi_corp_staging.gtdtmae m
LEFT JOIN bi_corp_staging.gtdtrgb gb ON m.gttcmae_mae_num_garantia = gb.gttcrgb_rgb_num_garantia AND gb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
LEFT JOIN bi_corp_staging.gtdtbie b ON b.gttcbie_bie_num_bien = gb.gttcrgb_rgb_num_bien AND b.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
LEFT JOIN bi_corp_staging.gtdttas t ON  t.gttctas_tas_num_bien = b.gttcbie_bie_num_bien AND t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
LEFT JOIN bi_corp_staging.gtdtcod c ON  c.cod_cod_codigo = m.gttcmae_mae_cod_garantia AND c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
LEFT JOIN bi_corp_staging.gtdtvab v ON  v.gttcvab_vab_num_bien = b.gttcbie_bie_num_bien AND v.gttcvab_vab_tip_valor = '015' AND v.partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
LEFT JOIN bi_corp_bdr.cotizaciones tmp ON tmp.cod_divisa = m.gttcmae_mae_cod_divisa
WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
AND gb.gttcrgb_rgb_fec_inivali <= gb.partition_date
AND gb.gttcrgb_rgb_fec_finvali >= gb.partition_date
-- AND c.partition_date = '2020-05-20' -- PREGUNTAR A JUANI PORQUE ESTA FECHA, RESOLVER LUEGO DE ESTE PERIODO
AND gttcmae_mae_cod_estado = '020'
AND c.cod_cod_tipo = '74'
AND c.cod_des_textored = '002'
AND c.cod_cod_entidad = '0072') t
;
