SET hive.merge.mapfiles=TRUE;
SET hive.merge.mapredfiles=TRUE;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT  overwrite TABLE bi_corp_business.tbl_cheques PARTITION (cod_kpi_kpi, partition_date)


SELECT

       chq.cod_chq_segmento                                                     AS ds_kpi_segmento,
       chq.cod_chq_subsegmento                                                  AS ds_kpi_subsegmento,
       chq.cod_chq_producto                                                     AS cod_kpi_producto,
       chq.cod_chq_subproducto                                                  AS cod_kpi_subproducto,
       chq.cod_chq_canal                                                        AS cod_kpi_canal,
       chq.ds_chq_destinofondos                                                 AS ds_kpi_destino,
       NULL                                                                     AS cod_kpi_zona,
       chq.cod_suc_sucursal                                                     AS cod_kpi_sucursal,
       chq.cod_chq_divisa                                                       AS cod_kpi_divisa,
       chq.fc_kpi_metrica                                                 AS fc_kpi_metrica,
       mismodiamesanterior.fc_kpi_metricamismodiamesanterior                    AS fc_kpi_metricamismodiamesanterior,
       tmenos1.fc_kpi_metricadiaanterior                                        AS fc_kpi_metricadiaanterior,
       findemesanterior.fc_kpi_metricafindemesanterior                          AS fc_kpi_metricafindemesanterior,
       mismodiaanoanterior.fc_kpi_metricamismodiaanoanterior                    AS fc_kpi_metricamismodiaanoanterior,
       findeanoanterior.fc_kpi_metricafindeanoanterior                          AS fc_kpi_metricafindeanoanterior,
       semanaanterior.fc_kpi_metricasemanaanterior                              AS fc_kpi_metricasemanaanterior,
       chq.fc_kpi_clientes                                                      AS fc_kpi_clientes,
       mismodiamesanterior.fc_kpi_clientesmismodiamesanterior                   AS fc_kpi_clientesmismodiamesanterior,
       tmenos1.fc_kpi_clientesdiaanterior                                       AS fc_kpi_clientesdiaanterior,
       findemesanterior.fc_kpi_clientesfindemesanterior                         AS fc_kpi_clientesfindemesanterior,
       mismodiaanoanterior.fc_kpi_clientesmismodiaanoanterior                   AS fc_kpi_clientesmismodiaanoanterior,
       findeanoanterior.fc_kpi_clientesfindeanoanterior                         AS fc_kpi_clientesfindeanoanterior,
       semanaanterior.fc_kpi_clientessemanaanterior                             AS fc_kpi_clientessemanaanterior,
       chq.fc_kpi_operaciones                                                    AS fc_kpi_operaciones,
       mismodiamesanterior.fc_kpi_operacionesmismodiamesanterior                AS fc_kpi_operacionesmismodiamesanterior,
       tmenos1.fc_kpi_operacionesdiaanterior                                    AS fc_kpi_operacionesdiaanterior,
       findemesanterior.fc_kpi_operacionesfindemesanterior                      AS fc_kpi_operacionesfindemesanterior,
       mismodiaanoanterior.fc_kpi_operacionesmismodiaanoanterior                AS fc_kpi_operacionesmismodiaanoanterior,
       findeanoanterior.fc_kpi_operacionesfindeanoanterior                      AS fc_kpi_operacionesfindeanoanterior,
       semanaanterior.fc_kpi_operacionessemanaanterior                          AS fc_kpi_operacionessemanaanterior,
       '000001'                                                              AS cod_kpi_kpi,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cheques-Daily') }}' as partition_date


FROM

(
  Select

  CASE WHEN c.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE c.cod_chq_segmento END as cod_chq_segmento,
  CASE WHEN c.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE c.cod_chq_subsegmento END as cod_chq_subsegmento,
  CASE WHEN c.cod_chq_producto    IS NULL THEN  'sin dato' ELSE c.cod_chq_producto END as cod_chq_producto,
  CASE WHEN c.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE c.cod_chq_subproducto END as cod_chq_subproducto,
  CASE WHEN c.cod_chq_canal       IS NULL THEN  'sin dato' ELSE c.cod_chq_canal END as cod_chq_canal,
  CASE WHEN c.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE c.ds_chq_destinofondos END  ds_chq_destinofondos,
  CASE WHEN c.cod_suc_sucursal    IS NULL THEN  999        ELSE c.cod_suc_sucursal END as cod_suc_sucursal,
  CASE WHEN c.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE c.cod_chq_divisa END as cod_chq_divisa,
  SUM (c.fc_chq_importecheque)                                                  as fc_kpi_metrica,
  COUNT (DISTINCT c.cod_per_nup)                                                as fc_kpi_clientes,
  COUNT (c.cod_chq_nrocheque)                                                   as fc_kpi_operaciones
  FROM bi_corp_common.stk_chq_cesionados c
  WHERE
    c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cheques-Daily') }}'
    AND c.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cheques-Daily') }}'
    AND c.cod_chq_evento = 'ALTA'
    GROUP BY
    c.cod_chq_segmento,
    c.cod_chq_subsegmento,
    c.cod_chq_producto,
    c.cod_chq_subproducto,
    c.cod_chq_canal,
    c.ds_chq_destinofondos,
    c.cod_suc_sucursal,
    c.cod_chq_divisa,
    c.partition_date
) chq
------- left previous_date_from
FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      sum(chq.fc_chq_importecheque)     as fc_kpi_metricadiaanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientesdiaanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionesdiaanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
  ) tmenos1

  ON  tmenos1.cod_chq_segmento    = chq.cod_chq_segmento
  and tmenos1.cod_chq_subsegmento = chq.cod_chq_subsegmento
  and tmenos1.cod_chq_producto    = chq.cod_chq_producto
  and tmenos1.cod_chq_subproducto = chq.cod_chq_subproducto
  and tmenos1.cod_chq_canal       = chq.cod_chq_canal
  and tmenos1.ds_chq_destinofondos = chq.ds_chq_destinofondos
  and tmenos1.cod_suc_sucursal    = chq.cod_suc_sucursal
  and tmenos1.cod_chq_divisa      = chq.cod_chq_divisa



FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      SUM(chq.fc_chq_importecheque)     as fc_kpi_metricafindemesanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientesfindemesanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionesfindemesanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
      ) findemesanterior

      on findemesanterior.cod_chq_segmento      = chq.cod_chq_segmento
      and findemesanterior.cod_chq_subsegmento  = chq.cod_chq_subsegmento
      and findemesanterior.cod_chq_producto     = chq.cod_chq_producto
      and findemesanterior.cod_chq_subproducto  = chq.cod_chq_subproducto
      and findemesanterior.cod_chq_canal        = chq.cod_chq_canal
      and findemesanterior.ds_chq_destinofondos = chq.ds_chq_destinofondos
      and findemesanterior.cod_suc_sucursal     = chq.cod_suc_sucursal
      and findemesanterior.cod_chq_divisa       = chq.cod_chq_divisa


------- left previous_week_to
FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      sum(chq.fc_chq_importecheque)     as fc_kpi_metricasemanaanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientessemanaanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionessemanaanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
      ) semanaanterior

  ON semanaanterior.cod_chq_segmento = chq.cod_chq_segmento
  and semanaanterior.cod_chq_subsegmento = chq.cod_chq_subsegmento
  and semanaanterior.cod_chq_producto    = chq.cod_chq_producto
  and semanaanterior.cod_chq_subproducto = chq.cod_chq_subproducto
  and semanaanterior.cod_chq_canal       = chq.cod_chq_canal
  and semanaanterior.ds_chq_destinofondos = chq.ds_chq_destinofondos
  and semanaanterior.cod_suc_sucursal    = chq.cod_suc_sucursal
  and semanaanterior.cod_chq_divisa      = chq.cod_chq_divisa


------- left previous_month_working_day
FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      sum(chq.fc_chq_importecheque)     as fc_kpi_metricamismodiamesanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientesmismodiamesanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionesmismodiamesanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
      ) mismodiamesanterior

  ON mismodiamesanterior.cod_chq_segmento = chq.cod_chq_segmento
  and mismodiamesanterior.cod_chq_subsegmento = chq.cod_chq_subsegmento
  and mismodiamesanterior.cod_chq_producto    = chq.cod_chq_producto
  and mismodiamesanterior.cod_chq_subproducto = chq.cod_chq_subproducto
  and mismodiamesanterior.cod_chq_canal       = chq.cod_chq_canal
  and mismodiamesanterior.ds_chq_destinofondos = chq.ds_chq_destinofondos
  and mismodiamesanterior.cod_suc_sucursal    = chq.cod_suc_sucursal
  and mismodiamesanterior.cod_chq_divisa      = chq.cod_chq_divisa


------- left previous_year_working_day
FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      sum(chq.fc_chq_importecheque)     as fc_kpi_metricamismodiaanoanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientesmismodiaanoanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionesmismodiaanoanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
      ) mismodiaanoanterior

  ON mismodiaanoanterior.cod_chq_segmento = chq.cod_chq_segmento
  and mismodiaanoanterior.cod_chq_subsegmento = chq.cod_chq_subsegmento
  and mismodiaanoanterior.cod_chq_producto    = chq.cod_chq_producto
  and mismodiaanoanterior.cod_chq_subproducto = chq.cod_chq_subproducto
  and mismodiaanoanterior.cod_chq_canal       = chq.cod_chq_canal
  and mismodiaanoanterior.ds_chq_destinofondos = chq.ds_chq_destinofondos
  and mismodiaanoanterior.cod_suc_sucursal    = chq.cod_suc_sucursal
  and mismodiaanoanterior.cod_chq_divisa      = chq.cod_chq_divisa


------- left previous_year_last_working_day
FULL OUTER JOIN
  (
    SELECT
    CASE WHEN chq.cod_chq_segmento    IS NULL THEN  'sin dato' ELSE chq.cod_chq_segmento END as cod_chq_segmento,
    CASE WHEN chq.cod_chq_subsegmento IS NULL THEN  'sin dato' ELSE chq.cod_chq_subsegmento END as cod_chq_subsegmento,
    CASE WHEN chq.cod_chq_producto    IS NULL THEN  'sin dato' ELSE chq.cod_chq_producto END as cod_chq_producto,
    CASE WHEN chq.cod_chq_subproducto IS NULL THEN  'sin dato' ELSE chq.cod_chq_subproducto END as cod_chq_subproducto,
    CASE WHEN chq.cod_chq_canal       IS NULL THEN  'sin dato' ELSE chq.cod_chq_canal END as cod_chq_canal,
    CASE WHEN chq.ds_chq_destinofondos IS NULL THEN  'sin dato' ELSE chq.ds_chq_destinofondos END  ds_chq_destinofondos,
    CASE WHEN chq.cod_suc_sucursal    IS NULL THEN  999        ELSE chq.cod_suc_sucursal END as cod_suc_sucursal,
    CASE WHEN chq.cod_chq_divisa      IS NULL THEN  'sin dato' ELSE chq.cod_chq_divisa END as cod_chq_divisa,
      sum(chq.fc_chq_importecheque)     as fc_kpi_metricafindeanoanterior,
      COUNT (DISTINCT chq.cod_per_nup)  as fc_kpi_clientesfindeanoanterior,
      COUNT (chq.cod_chq_nrocheque)     as fc_kpi_operacionesfindeanoanterior
    FROM
      bi_corp_common.stk_chq_cesionados chq
    WHERE
      partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.dt_chq_fechalectura = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Cheques-Daily') }}'
      AND chq.cod_chq_evento = 'ALTA'
    GROUP BY
      chq.cod_chq_segmento,
      chq.cod_chq_subsegmento,
      chq.cod_chq_producto,
      chq.cod_chq_subproducto,
      chq.cod_chq_canal,
      chq.ds_chq_destinofondos,
      chq.cod_suc_sucursal,
      chq.cod_chq_divisa
      ) findeanoanterior

  ON findeanoanterior.cod_chq_segmento = chq.cod_chq_segmento
  and findeanoanterior.cod_chq_subsegmento = chq.cod_chq_subsegmento
  and findeanoanterior.cod_chq_producto    = chq.cod_chq_producto
  and findeanoanterior.cod_chq_subproducto = chq.cod_chq_subproducto
  and findeanoanterior.cod_chq_canal       = chq.cod_chq_canal
  and findeanoanterior.ds_chq_destinofondos = chq.ds_chq_destinofondos
  and findeanoanterior.cod_suc_sucursal    = chq.cod_suc_sucursal
  and findeanoanterior.cod_chq_divisa      = chq.cod_chq_divisa

;
