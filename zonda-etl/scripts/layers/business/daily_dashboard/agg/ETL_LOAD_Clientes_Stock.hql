

CREATE TEMPORARY TABLE seg_personas AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
 WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE cta_orden_preembozadas AS

  SELECT
          num_cuenta,
          cod_producto,
          cod_subprodu,
          cod_centro
    FROM
      (select num_cuenta, cod_producto, cod_subprodu, cod_centro, cod_rubro_bcra,
        case
        when cod_producto in ('35', '37', '40', '41', '42') and deuda > 1 then 1
        when cod_producto not in ('35', '37', '40', '41', '42') then 1 else 0 end as Flag_2
    FROM
        (
        select  cod_centro, num_cuenta, cod_producto, cod_subprodu, cod_rubro_bcra,
        sum(imp_deuda) deuda
        from santander_business_risk_arda.contratos_adsf
        group by cod_centro, num_cuenta, cod_producto, cod_subprodu, cod_rubro_bcra
        ) adsf
    )X
    WHERE Flag_2 = 0 or cod_rubro_bcra in ('711046', '715046');

CREATE TEMPORARY TABLE RCC_use AS
    SELECT
    rcc.num_persona,
    rcc.num_cuenta,
    rcc.cod_subprodu,
    rcc.cod_centro,
    rcc.data_date_part

  FROM rcc
  left join cta_orden_preembozadas cta
  on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
  WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

----------previous_date_from
CREATE TEMPORARY TABLE seg_personas_previous_date_from AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
 WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_date_from AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_date_from AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_previous_date_from rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_date_from AS

  SELECT
  ds_kpi_segmento,
  ds_kpi_subsegmento,
  p.cod_sucursal_administradora             AS cod_kpi_sucursal,
  COUNT (distinct p.num_persona)            AS fc_kpi_clientesdiaanterior
  FROM santander_business_risk_arda.personas p
  inner join RCC_use_previous_date_from
  on p.num_persona = RCC_use_previous_date_from.num_persona
  left join seg_personas_previous_date_from
  on p.num_persona = seg_personas_previous_date_from.penumper
  Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_BSN_Daily_Dashboard') }}'
  and p.condicion_cliente = 'CLI'
  group by
  ds_kpi_segmento,
  ds_kpi_subsegmento,
  p.cod_sucursal_administradora;

--------- previous_month_to
CREATE TEMPORARY TABLE seg_personas_previous_month_to AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
    WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_month_to AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_month_to AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_month_to rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_month_to AS

        SELECT
        ds_kpi_segmento,
        ds_kpi_subsegmento,
        p.cod_sucursal_administradora             AS cod_kpi_sucursal,
        COUNT (distinct p.num_persona)            AS fc_kpi_clientesfindemesanterior
        FROM santander_business_risk_arda.personas p
        inner join RCC_use_previous_month_to
        on p.num_persona = RCC_use_previous_month_to.num_persona
        left join seg_personas_previous_month_to
        on p.num_persona = seg_personas_previous_month_to.penumper
        Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_BSN_Daily_Dashboard') }}'
        and p.condicion_cliente = 'CLI'
        group by
        ds_kpi_segmento,
        ds_kpi_subsegmento,
        p.cod_sucursal_administradora;

--------- previous_week_to
CREATE TEMPORARY TABLE seg_personas_previous_week_to AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
    WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_week_to AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_to', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_week_to AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_week_to rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_week_to AS

              SELECT
              ds_kpi_segmento,
              ds_kpi_subsegmento,
              p.cod_sucursal_administradora             AS cod_kpi_sucursal,
              COUNT (distinct p.num_persona)            AS fc_kpi_clientessemanaanterior
              FROM santander_business_risk_arda.personas p
              inner join RCC_use_previous_week_to
              on p.num_persona = RCC_use_previous_week_to.num_persona
              left join seg_personas_previous_week_to
              on p.num_persona = seg_personas_previous_week_to.penumper
              Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_to', dag_id='LOAD_BSN_Daily_Dashboard') }}'
              and p.condicion_cliente = 'CLI'
              group by
              ds_kpi_segmento,
              ds_kpi_subsegmento,
              p.cod_sucursal_administradora;

-------- previous_month_working_day
CREATE TEMPORARY TABLE seg_personas_previous_month_working_day AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
    WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_month_working_day AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_month_working_day AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_month_working_day rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_month_working_day AS

                    SELECT
                    ds_kpi_segmento,
                    ds_kpi_subsegmento,
                    p.cod_sucursal_administradora             AS cod_kpi_sucursal,
                    COUNT (distinct p.num_persona)            AS fc_kpi_clientesmismodiamesanterior
                    FROM santander_business_risk_arda.personas p
                    inner join RCC_use_previous_month_working_day
                    on p.num_persona = RCC_use_previous_month_working_day.num_persona
                    left join seg_personas_previous_month_working_day
                    on p.num_persona = seg_personas_previous_month_working_day.penumper
                    Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}'
                    and p.condicion_cliente = 'CLI'
                    group by
                    ds_kpi_segmento,
                    ds_kpi_subsegmento,
                    p.cod_sucursal_administradora;

-------- previous_year_working_day
CREATE TEMPORARY TABLE seg_personas_previous_year_working_day AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
    WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_year_working_day AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_year_working_day AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_year_working_day rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_year_working_day AS

                          SELECT
                          ds_kpi_segmento,
                          ds_kpi_subsegmento,
                          p.cod_sucursal_administradora             AS cod_kpi_sucursal,
                          COUNT (distinct p.num_persona)            AS fc_kpi_clientesmismodiaanoanterior
                          FROM santander_business_risk_arda.personas p
                          inner join RCC_use_previous_year_working_day
                          on p.num_persona = RCC_use_previous_year_working_day.num_persona
                          left join seg_personas_previous_year_working_day
                          on p.num_persona = seg_personas_previous_year_working_day.penumper
                          Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}'
                          and p.condicion_cliente = 'CLI'
                          group by
                          ds_kpi_segmento,
                          ds_kpi_subsegmento,
                          p.cod_sucursal_administradora;

-------- previous_year_last_working_day
CREATE TEMPORARY TABLE seg_personas_previous_year_last_working_day AS

  SELECT
    penumper,
    CASE WHEN SUBSTR(pesegcla,1,1) IN ('6','7','8','A','B','C','D','E','F','G','H','I') THEN 'INDIVIDUOS'
      WHEN SUBSTR(pesegcla,1,1) IN ('5','4','J','K','L','M') THEN 'PYME'
      WHEN SUBSTR(pesegcla,1,1) IN ('N','O','P','Q','3','9','0') THEN 'BEIA'
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_segmento,
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
      WHEN SUBSTR(pesegcla,1,1) IN ('R','S','T','U','V','W','X','Y','Z','1','2') THEN 'BMA' END            AS ds_kpi_subsegmento

    FROM bi_corp_staging.malpe_pedt030
    WHERE  partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE rcc_previous_year_last_working_day AS

  SELECT
    r.num_persona,
    r.num_cuenta,
    r.cod_producto,
    r.cod_subprodu,
    r.cod_centro,
    r.data_date_part
  FROM santander_business_risk_arda.relacion_contrato_cliente r
  WHERE
    r.cal_participacion = 'TI'
    and r.cod_estado_relacion = 'A'
    and r.cod_producto !='83'
    and concat(substr(r.data_date_part , 1, 4), '-',substr(r.data_date_part , 5, 2), '-',SUBSTR (r.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}';

CREATE TEMPORARY TABLE RCC_use_previous_year_last_working_day AS
        SELECT
        rcc.num_persona,
        rcc.num_cuenta,
        rcc.cod_subprodu,
        rcc.cod_centro,
        rcc.data_date_part

      FROM rcc_previous_year_last_working_day rcc
      left join cta_orden_preembozadas cta
      on concat (rcc.num_cuenta,rcc.cod_producto,rcc.cod_subprodu,rcc.cod_centro)   = concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro)
      WHERE concat (cta.num_cuenta,cta.cod_producto,cta.cod_subprodu,cta.cod_centro) is null ;

CREATE TEMPORARY TABLE metrics_previous_year_last_working_day AS

                                SELECT
                                ds_kpi_segmento,
                                ds_kpi_subsegmento,
                                p.cod_sucursal_administradora             AS cod_kpi_sucursal,
                                COUNT (distinct p.num_persona)            AS fc_kpi_clientesfindeanoanterior
                                FROM santander_business_risk_arda.personas p
                                inner join RCC_use_previous_year_last_working_day
                                on p.num_persona = RCC_use_previous_year_last_working_day.num_persona
                                left join seg_personas_previous_year_last_working_day
                                on p.num_persona = seg_personas_previous_year_last_working_day.penumper
                                Where concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_year_last_working_day', dag_id='LOAD_BSN_Daily_Dashboard') }}'
                                and p.condicion_cliente = 'CLI'
                                group by
                                ds_kpi_segmento,
                                ds_kpi_subsegmento,
                                p.cod_sucursal_administradora;



INSERT  overwrite TABLE bi_corp_business.agg_all_daily_dashboard PARTITION (cod_kpi_kpi, partition_date)

  SELECT
                                                                                                                  seg_p.ds_kpi_segmento,
                                                                                                                  seg_p.ds_kpi_subsegmento,
       NULL                                                                                                       AS cod_kpi_producto,
       NULL                                                                                                       AS cod_kpi_subproducto,
       NULL                                                                                                       AS cod_kpi_canal,
       NULL                                                                                                       AS ds_kpi_destino,
       NULL                                                                                                       AS cod_kpi_zona,
       p.cod_sucursal_administradora                                                                              AS cod_kpi_sucursal,
       NULL                                                                                                       AS cod_kpi_divisa,
       NULL                                                                                                       AS fc_kpi_metrica,
       NULL                                                                                                       AS fc_kpi_metricamismodiamesanterior,
       NULL                                                                                                       AS fc_kpi_metricadiaanterior,
       NULL                                                                                                       AS fc_kpi_metricafindemesanterior,
       NULL                                                                                                       AS fc_kpi_metricamismodiaanoanterior,
       NULL                                                                                                       AS fc_kpi_metricafindeanoanterior,
       NULL                                                                                                       AS fc_kpi_metricasemanaanterior,
       COUNT (distinct p.num_persona)                                                                             AS fc_kpi_clientes,
       SUM (metrics_previous_month_working_day.fc_kpi_clientesmismodiamesanterior)                                AS fc_kpi_clientesmismodiamesanterior,
       SUM (metrics_previous_date_from.fc_kpi_clientesdiaanterior)                                                AS fc_kpi_clientesdiaanterior,
       SUM (metrics_previous_month_to.fc_kpi_clientesfindemesanterior)                                            AS fc_kpi_clientesfindemesanterior,
       SUM (metrics_previous_year_working_day.fc_kpi_clientesmismodiaanoanterior)                                 AS fc_kpi_clientesmismodiaanoanterior,
       SUM (metrics_previous_year_last_working_day.fc_kpi_clientesfindeanoanterior)                               AS fc_kpi_clientesfindeanoanterior,
       SUM (metrics_previous_week_to.fc_kpi_clientessemanaanterior)                                               AS fc_kpi_clientessemanaanterior,
       NULL                                                                                                       AS fc_kpi_operaciones,
       NULL                                                                                                       AS fc_kpi_operacionesmismodiamesanterior,
       NULL                                                                                                       AS fc_kpi_operacionesdiaanterior,
       NULL                                                                                                       AS fc_kpi_operacionesfindemesanterior,
       NULL                                                                                                       AS fc_kpi_operacionesmismodiaanoanterior,
       NULL                                                                                                       AS fc_kpi_operacionesfindeanoanterior,
       NULL                                                                                                       AS fc_kpi_operacionessemanaanterior,
       '001001005'                                                                                                AS cod_kpi_kpi,
       p.data_date_part

       FROM santander_business_risk_arda.personas p

       inner join RCC_use
       on p.num_persona = RCC_use.num_persona

       LEFT JOIN  seg_personas seg_p
       on p.num_persona = seg_p.penumper

       LEFT JOIN
        metrics_previous_date_from
        ON metrics_previous_date_from.ds_kpi_segmento     = seg_p.ds_kpi_segmento
        AND metrics_previous_date_from.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
        AND metrics_previous_date_from.cod_kpi_sucursal   = p.cod_sucursal_administradora

      LEFT JOIN
       metrics_previous_month_to
       ON metrics_previous_month_to.ds_kpi_segmento     = seg_p.ds_kpi_segmento
       AND metrics_previous_month_to.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
       AND metrics_previous_month_to.cod_kpi_sucursal   = p.cod_sucursal_administradora

      LEFT JOIN
       metrics_previous_week_to
       ON metrics_previous_week_to.ds_kpi_segmento     = seg_p.ds_kpi_segmento
       AND metrics_previous_week_to.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
       AND metrics_previous_week_to.cod_kpi_sucursal   = p.cod_sucursal_administradora

      LEFT JOIN
       metrics_previous_month_working_day
       ON metrics_previous_month_working_day.ds_kpi_segmento     = seg_p.ds_kpi_segmento
       AND metrics_previous_month_working_day.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
       AND metrics_previous_month_working_day.cod_kpi_sucursal   = p.cod_sucursal_administradora

     LEFT JOIN
      metrics_previous_year_working_day
      ON metrics_previous_year_working_day.ds_kpi_segmento     = seg_p.ds_kpi_segmento
      AND metrics_previous_year_working_day.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
      AND metrics_previous_year_working_day.cod_kpi_sucursal   = p.cod_sucursal_administradora

     LEFT JOIN
      metrics_previous_year_last_working_day
      ON metrics_previous_year_last_working_day.ds_kpi_segmento     = seg_p.ds_kpi_segmento
      AND metrics_previous_year_last_working_day.ds_kpi_subsegmento = seg_p.ds_kpi_subsegmento
      AND metrics_previous_year_last_working_day.cod_kpi_sucursal   = p.cod_sucursal_administradora


    WHERE
       concat(substr(p.data_date_part , 1, 4), '-',substr(p.data_date_part , 5, 2), '-',SUBSTR (p.data_date_part , (-2)))= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Daily_Dashboard') }}'
       and p.condicion_cliente = 'CLI'
       GROUP BY
       seg_p.ds_kpi_segmento,
       seg_p.ds_kpi_subsegmento,
       p.cod_sucursal_administradora,
       p.data_date_part;
