SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

----------- temporales partition_date
CREATE TEMPORARY TABLE seg AS
  SELECT  penumper as cod_per_nup,

          CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
	         WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
	         WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
	         WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_segmento,
          CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8') THEN 'SELECT'
	         WHEN SUBSTR(pesegcla,1,1) IN ('A','B','C') THEN 'RENTA ALTA'
	         WHEN SUBSTR(pesegcla,1,1) IN ('D','E','F') THEN 'RENTA MEDIA'
	         WHEN SUBSTR(pesegcla,1,1) IN ('G','H','I') THEN 'RENTA MASIVA'
	         WHEN SUBSTR(pesegcla,1,1) IN ('5','4') THEN 'DUO'
	         WHEN SUBSTR(pesegcla,1,1) IN ('J','K') THEN 'PYME 1'
	         WHEN SUBSTR(pesegcla,1,1) IN ('L','M') THEN 'PYME 2'
	         WHEN SUBSTR(pesegcla,1,1) IN ('N','O') THEN 'EMPRESA'
	         WHEN SUBSTR(pesegcla,1,1) IN ('P','Q') THEN 'GRAN EMPRESA' -- G1
	         WHEN SUBSTR(pesegcla,1,1) IN ('3','9','0') THEN 'INSTITUCIONES'
	         WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END ds_per_subsegmento
  FROM bi_corp_staging.malpe_pedt030 pedt030
  --JOIN max_030
  WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE malpe008_relation AS
  SELECT aa.penumper,
       aa.pecodofi,
       aa.penumcon,
       aa.pecodpro,
       aa.pecodsub,
       aa.partition_date
  FROM
    (SELECT row_number() OVER (PARTITION BY p08.pecodofi,
                                          p08.penumcon
                             ORDER BY p08.peordpar) AS orden,
                            p08.penumper,
                            p08.pecodofi,
                            p08.penumcon,
                            p08.pecodpro,
                            p08.pecodsub,
                            p08.partition_date
    FROM bi_corp_staging.malpe_pedt008 p08

    --JOIN max_030 mx8
    --ON p08.partition_date = mx8.partition_date

    WHERE
    p08.partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}'
     AND p08.pecalpar = 'TI'
     AND p08.peestrel = 'A'
     AND p08.pefecbrb = '9999-12-31'
     AND p08.pecodpro IN ('02',
                          '03',
                          '05',
                          '06',
                          '07')) aa
  WHERE aa.orden = 1 ;

CREATE TEMPORARY TABLE tmenos1 AS
  SELECT
    cod_kpi_segmento,
    cod_kpi_subsegmento,
    ds_kpi_producto,
    ds_kpi_subproducto,
    cod_kpi_sucursal,
    cod_kpi_divisa,
    fc_kpi_metrica,
    fc_kpi_clientes
  FROM
    bi_corp_business.agg_all_daily_dashboard
  WHERE cod_kpi_kpi = '001001001'
    and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE findemesanterior AS
      SELECT
        cod_kpi_segmento,
    cod_kpi_subsegmento,
    ds_kpi_producto,
    ds_kpi_subproducto,
        cod_kpi_sucursal,
        cod_kpi_divisa,
        fc_kpi_metrica,
        fc_kpi_clientes
      FROM
        bi_corp_business.agg_all_daily_dashboard
      WHERE cod_kpi_kpi = '001001001'
        and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE semanaanterior AS
        SELECT
             cod_kpi_segmento,
    cod_kpi_subsegmento,
    ds_kpi_producto,
    ds_kpi_subproducto,
                cod_kpi_sucursal,
                cod_kpi_divisa,
                fc_kpi_metrica,
                fc_kpi_clientes
              FROM
                bi_corp_business.agg_all_daily_dashboard
              WHERE cod_kpi_kpi = '001001001'
                and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE mismodiamesanterior AS
            SELECT
                cod_kpi_segmento,
    cod_kpi_subsegmento,
    ds_kpi_producto,
    ds_kpi_subproducto,
                cod_kpi_sucursal,
                cod_kpi_divisa,
                fc_kpi_metrica,
                fc_kpi_clientes
              FROM
                bi_corp_business.agg_all_daily_dashboard
              WHERE cod_kpi_kpi = '001001001'
                and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE mismodiaanoanterior AS
          SELECT
          cod_kpi_segmento,
    cod_kpi_subsegmento,
    ds_kpi_producto,
    ds_kpi_subproducto,
            cod_kpi_sucursal,
            cod_kpi_divisa,
            fc_kpi_metrica,
            fc_kpi_clientes
          FROM
            bi_corp_business.agg_all_daily_dashboard
          WHERE cod_kpi_kpi = '001001001'
            and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE findeanoanterior AS
          SELECT
            cod_kpi_segmento,
            cod_kpi_subsegmento,
            ds_kpi_producto,
            ds_kpi_subproducto,
            cod_kpi_sucursal,
            cod_kpi_divisa,
            fc_kpi_metrica,
            fc_kpi_clientes
          FROM
              bi_corp_business.agg_all_daily_dashboard
          WHERE cod_kpi_kpi = '001001001'
            and partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

INSERT  overwrite TABLE bi_corp_business.agg_all_daily_dashboard PARTITION (cod_kpi_kpi, partition_date)

--cod_kpi_segmento,
         --       cod_kpi_subsegmento,
SELECT

       per_use.ds_per_segmento                                                                                    AS cod_kpi_segmento,
       per_use.ds_per_subsegmento                                                                                 AS cod_kpi_subsegmento,
       mae.producto                                                                                               AS ds_kpi_producto,
       mae.subprodu                                                                                               AS ds_kpi_subproducto,
       NULL                                                                                                       AS ds_kpi_canal,
       NULL                                                                                                       AS ds_kpi_destino,
       NULL                                                                                                       AS cod_kpi_zona,
       mae.centro_alta                                                                                            AS cod_kpi_sucursal,
       mae.divisa                                                                                                 AS cod_kpi_divisa,
       sum (cast(concat(substr(mae.saldo_dispue, 1, 13), '.', SUBSTR (mae.saldo_dispue, (-2))) AS decimal(15, 2))) AS fc_kpi_metrica,
       sum (mismodiamesanterior.fc_kpi_metrica)                                                                   AS fc_kpi_metricamismodiamesanterior,
       sum (tmenos1.fc_kpi_metrica)                                                                               AS fc_kpi_metricadiaanterior,
       sum (findemesanterior.fc_kpi_metrica)                                                                      AS fc_kpi_metricafindemesanterior,
       sum (mismodiaanoanterior.fc_kpi_metrica)                                                                   AS fc_kpi_metricamismodiaanoanterior,
       sum (findeanoanterior.fc_kpi_metrica)                                                                      AS fc_kpi_metricafindeanoanterior,
       sum (semanaanterior.fc_kpi_metrica)                                                                        AS fc_kpi_metricasemanaanterior,
       COUNT (DISTINCT malpe_re.penumper)                                                                         AS fc_kpi_clientes,
       sum (mismodiamesanterior.fc_kpi_clientes)                                                                  AS fc_kpi_clientesmismodiamesanterior,
       sum (tmenos1.fc_kpi_clientes)                                                                              AS fc_kpi_clientesdiaanterior,
       sum (findemesanterior.fc_kpi_clientes)                                                                     AS fc_kpi_clientesfindemesanterior,
       sum (mismodiaanoanterior.fc_kpi_clientes)                                                                  AS fc_kpi_clientesmismodiaanoanterior,
       sum (findeanoanterior.fc_kpi_clientes)                                                                     AS fc_kpi_clientesfindeanoanterior,
       sum (semanaanterior.fc_kpi_clientes)                                                                       AS fc_kpi_clientessemanaanterior,
       NULL                                                                                                       AS fc_kpi_operaciones,
       NULL                                                                                                       AS fc_kpi_operacionesmismodiamesanterior,
       NULL                                                                                                       AS fc_kpi_operacionesdiaanterior,
       NULL                                                                                                       AS fc_kpi_operacionesfindemesanterior,
       NULL                                                                                                       AS fc_kpi_operacionesmismodiaanoanterior,
       NULL                                                                                                       AS fc_kpi_operacionesfindeanoanterior,
       NULL                                                                                                       AS fc_kpi_operacionessemanaanterior,
       '001001001'                                                                                                AS cod_kpi_kpi,
       mae.partition_date

  FROM bi_corp_staging.bgdtmae mae

  INNER JOIN malpe008_relation malpe_re
    ON mae.centro_alta                          = malpe_re.pecodofi
    AND concat(substr(mae.cuenta, 1, 1), '00', SUBSTR (mae.cuenta, (-9))) = malpe_re.penumcon
    AND mae.producto                            = malpe_re.pecodpro
    AND mae.subprodu                            = malpe_re.pecodsub

    LEFT JOIN seg per_use
      ON per_use.cod_per_nup                      = malpe_re.penumper



    -------  previous_date_from
    FULL OUTER JOIN tmenos1
        on concat(per_use.ds_per_segmento,
          per_use.ds_per_subsegmento,
          mae.producto,
          mae.subprodu,
          mae.centro_alta,
          mae.divisa)                   =   concat(tmenos1.cod_kpi_segmento,
                                              tmenos1.cod_kpi_subsegmento,
                                              tmenos1.ds_kpi_producto,
                                              tmenos1.ds_kpi_subproducto,
                                              tmenos1.cod_kpi_sucursal,
                                              tmenos1.cod_kpi_divisa)




    -------  previous_month_to
    FULL OUTER JOIN
         findemesanterior
         on concat(per_use.ds_per_segmento,
              per_use.ds_per_subsegmento,
              mae.producto,
              mae.subprodu,
              mae.centro_alta,
              mae.divisa)                   =   concat(findemesanterior.cod_kpi_segmento,
                                                  findemesanterior.cod_kpi_subsegmento,
                                                  findemesanterior.ds_kpi_producto,
                                                  findemesanterior.ds_kpi_subproducto,
                                                  findemesanterior.cod_kpi_sucursal,
                                                  findemesanterior.cod_kpi_divisa)



    -------  previous_week_to
    FULL OUTER JOIN
         semanaanterior
         on concat(per_use.ds_per_segmento,
              per_use.ds_per_subsegmento,
              mae.producto,
              mae.subprodu,
              mae.centro_alta,
              mae.divisa)                   =   concat(semanaanterior.cod_kpi_segmento,
                                                  semanaanterior.cod_kpi_subsegmento,
                                                  semanaanterior.ds_kpi_producto,
                                                  semanaanterior.ds_kpi_subproducto,
                                                  semanaanterior.cod_kpi_sucursal,
                                                  semanaanterior.cod_kpi_divisa)



    -------  previous_month_working_day
    FULL OUTER JOIN
        mismodiamesanterior
        on concat(per_use.ds_per_segmento,
             per_use.ds_per_subsegmento,
             mae.producto,
             mae.subprodu,
             mae.centro_alta,
             mae.divisa)                   =   concat(mismodiamesanterior.cod_kpi_segmento,
                                                 mismodiamesanterior.cod_kpi_subsegmento,
                                                 mismodiamesanterior.ds_kpi_producto,
                                                 mismodiamesanterior.ds_kpi_subproducto,
                                                 mismodiamesanterior.cod_kpi_sucursal,
                                                 mismodiamesanterior.cod_kpi_divisa)




    -------  previous_year_working_day
    FULL OUTER JOIN
        mismodiaanoanterior
        on concat(per_use.ds_per_segmento,
             per_use.ds_per_subsegmento,
             mae.producto,
             mae.subprodu,
             mae.centro_alta,
             mae.divisa)                   =   concat(mismodiaanoanterior.cod_kpi_segmento,
                                                 mismodiaanoanterior.cod_kpi_subsegmento,
                                                 mismodiaanoanterior.ds_kpi_producto,
                                                 mismodiaanoanterior.ds_kpi_subproducto,
                                                 mismodiaanoanterior.cod_kpi_sucursal,
                                                 mismodiaanoanterior.cod_kpi_divisa)



    -------  previous_year_last_working_day
    FULL OUTER JOIN
      findeanoanterior
      on concat(per_use.ds_per_segmento,
           per_use.ds_per_subsegmento,
           mae.producto,
           mae.subprodu,
           mae.centro_alta,
           mae.divisa)                   =   concat(findeanoanterior.cod_kpi_segmento,
                                               findeanoanterior.cod_kpi_subsegmento,
                                               findeanoanterior.ds_kpi_producto,
                                               findeanoanterior.ds_kpi_subproducto,
                                               findeanoanterior.cod_kpi_sucursal,
                                               findeanoanterior.cod_kpi_divisa)


WHERE
      mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}'
      AND cast(concat(substr(mae.saldo_dispue, 1, 13), '.', SUBSTR (mae.saldo_dispue, (-2))) AS decimal(15, 2)) > 0
      AND mae.producto IN ('02',
                           '03',
                           '05',
                           '06',
                           '07')
      AND mae.indesta = 'A'
      AND mae.saldo_dispue_sgn = '+'

    GROUP BY
             per_use.ds_per_segmento,
             per_use.ds_per_subsegmento,
             mae.producto,
             mae.subprodu,
             mae.centro_alta,
             mae.divisa,
             mae.partition_date;