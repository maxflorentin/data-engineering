insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0003321
-- La fecha de apertura del contrato no puede ser mayor que la fecha de vencimiento original del mismo
SELECT t.g4095_feoperac as fecha_proceso,
    'R0003321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper > t.G4095_FECVENTO then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0732321
-- La fecha de apertura del contrato no puede ser mayor que la fecha de vencimiento actual del mismo
SELECT t.g4095_feoperac as fecha_proceso,
    'R0732321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper < t.G4095_FECVE2 then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0004321
-- La fecha de cancelación del contrato no puede ser menor que la fecha de apertura del mismo
SELECT t.g4095_feoperac as fecha_proceso,
    'R0004321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper > t.G4095_FECCANCE then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0005321
-- Si la fecha de reestructuración, la fecha de refinanciación o la fecha de novación están informadas, estas no pueden ser menores que la fecha de apertura del contrato
select g4095_feoperac as fecha_proceso, --2019-07
       "R0005321" as cod_incidencia,
       cast(count(*) as string) as num_reg_total,
       sum(case when ((((
           if(((cast(regexp_replace(nvl(G4095_FECREES,G4095_FECHAPER),"-","") as int)-- as "fecha reestructuración"
              -
              cast(regexp_replace(G4095_FECHAPER,"-","") as int))-- as "fecha apertura contrato",
              >= 0),
              0,1) )
            +
             (
           if(((cast(regexp_replace(nvl(G4095_FECREFI,G4095_FECHAPER),"-","") as int)-- as "fecha refinanciación"
              -
              cast(regexp_replace(G4095_FECHAPER,"-","") as int))-- as "fecha apertura contrato",
              >= 0),
              0,1) )
              +
             (
           if(((cast(regexp_replace(nvl(G4095_FECNOVAC,G4095_FECHAPER),"-","") as int)-- as "fecha renovación"
              -
              cast(regexp_replace(G4095_FECHAPER,"-","") as int))-- as "fecha apertura contrato",
              >= 0),
              0,1) ))
           )
        )>0 then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
  from bi_corp_bdr.JM_CONTR_BIS
  where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
  group by g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0006321
-- Si el identificador de contrato padre está informado, dicho identificador debe de existir en la tabla de contratos
  select pivot.g4095_feoperac as fecha_proceso,
       "R0006321" as cod_incidencia,
       cast(count(*) as string) as num_reg_total,
       cast( SUM(case valid.g4095_contra1 when pivot.g4095_contra1 then 0 else 1 end) as string) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
  from bi_corp_bdr.JM_CONTR_BIS pivot
  left join bi_corp_bdr.JM_CONTR_BIS valid
       on  valid.partition_date = pivot.partition_date
       and valid.g4095_contra1 = pivot.g4095_contra1
  where pivot.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
    and pivot.g4095_contra1 <> '000000000'
  group by pivot.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0008321331
-- Si el indicador de titulizado está marcado con ‘S’, el identificador de contrato debe existir en la tabla de Titulizaciones
select pivot.g4095_feoperac as fecha_proceso, --2019-07
       "R0008321331" as cod_incidencia,
       cast(count(*) as string) as num_reg_total,
       cast( SUM(if(G4095_IND_TITU = "S",case match.contrato_bdr when pivot.g4095_contra1 then 0 else 1 end,0)) as string) as num_reg_error,    
       from_unixtime(unix_timestamp()) as op_timestamp
  from bi_corp_bdr.JM_CONTR_BIS pivot
  left join
            (
            select distinct(a.g4095_contra1) as contrato_bdr
              from bi_corp_bdr.jm_contr_bis a inner join 
                   bi_corp_bdr.jm_tituliz b on a.G4095_IND_TITU = "S"
               and a.g4095_contra1 = b.g4132_contra1
            ) match
        on match.contrato_bdr = pivot.g4095_contra1
  where pivot.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
  group by pivot.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0100321
-- No deben existir registros duplicados
  select pivot.g4095_feoperac as fecha_proceso, --2019-07
       "R0100321" as cod_incidencia,
       cast(count(*) as string) as num_reg_total,
       count(pivot.g4095_contra1) - count(distinct pivot.g4095_contra1) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
  from bi_corp_bdr.JM_CONTR_BIS pivot
    where pivot.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
  group by pivot.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0500321
-- El valor del campo Agrupador de Producto debe existir en la tabla de baremos correspondiente (3.2.1)
SELECT t.g4095_feoperac as fecha_proceso,
       'R0500321' as cod_incidencia,
       count(*) as num_reg_total,
       sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
       from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
         LEFT JOIN bi_corp_bdr.v_map_baremos_global_local bgl on
            bgl.cod_negocio_local = 3 and
            bgl.cod_baremo_local = t.G4095_ID_PROD
where t.partition_date = '${month_partition_bdr}'
group by t.g4095_feoperac;

--R0015321
-- Por tomar el perímetro del tablón de contratos este indicador se encuentra validado.

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--cod. incidencia: R0501321
-- El valor del campo Finalidad debe existir en la tabla de baremos correspondiente (3.2.1)
SELECT t.g4095_feoperac as fecha_proceso,
    'R0501321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local bgl
    ON bgl.cod_negocio_local = 4
   AND bgl.cod_baremo_global = lpad(t.G4095_IDFINALI,5,'0')
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha apertura debe ser menor o igual que la Fecha de proceso
SELECT t.g4095_feoperac as fecha_proceso,
    'R0793321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper > t.g4095_feoperac then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha vencimiento original debe ser distinta de ‘0001-01-01’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0812321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecvento = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha vencimiento actual debe ser distinta de ‘0001-01-01’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0813321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecve2 = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha apertura debe ser distinta de ‘0001-01-01’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0814321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha apertura debe ser distinta de ‘9999-12-31’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0815321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fechaper = '9999-12-31' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha cancelación debe ser distinta de ‘0001-01-01’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0816321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_feccance = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha reestructuración debe ser distinta de ‘9999-12-31’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0817321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecrees = '9999-12-31' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha refinanciación debe ser distinta de ‘9999-12-31’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0818321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecrefi = '9999-12-31' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha novación debe ser distinta de ‘9999-12-31’
SELECT t.g4095_feoperac as fecha_proceso,
    'R0819321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecnovac = '9999-12-31' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El contrato debe tener algún registro asociado en la tabla Contrato Otros Datos
SELECT t.g4095_feoperac as fecha_proceso,
    'R0046321325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_contra1 is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
LEFT JOIN bi_corp_bdr.jm_contr_otros co
on t.g4095_contra1 = co.e0623_contra1 and t.partition_date = co.partition_date
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La Fecha de vencimiento actual debe ser mayor o igual que la fecha de proceso
SELECT t.g4095_feoperac as fecha_proceso,
    'R0792321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_fecve2 < t.g4095_feoperac  then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El id de contrato no debe existir en la tabla de exposiciones no contractuales
SELECT t.g4095_feoperac as fecha_proceso,
    'R0001321326' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when expno.e0627_contra1 is not null  then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
LEFT JOIN bi_corp_bdr.jm_expos_no_con expno
on expno.e0627_contra1 = t.g4095_contra1 and expno.partition_date = t.partition_date
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El identificador de contrato debe existir en la tabla Calificación Contrato (3.2.1)
SELECT t.g4095_feoperac as fecha_proceso,
    'R0932321327' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_contra1 is null  then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
LEFT JOIN bi_corp_bdr.jm_cal_in_co cal
on cal.e9952_contra1 = t.g4095_contra1 and cal.partition_date = t.partition_date
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La fecha de Operación debe ser mayor o igual que la fecha de apertura del contrato
SELECT t.g4095_feoperac as fecha_proceso,
    'R0973321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when t.g4095_feoperac >= t.g4095_fechaper is null  then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
FROM bi_corp_bdr.jm_contr_bis t
where t.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by t.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El indicador de Aval Ejecutado debe contener un valor válido (3.2.1) R1227321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1227321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_indava not in ("S", "N", " ") then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El indicador de Titulizado debe contener un valor válido (3.2.1) R1228321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1228321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_ind_titu not in ("S", "N", " ") then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El indicador de Deuda Pública debe contener un valor válido (3.2.1) R1229321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1229321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_inddeud not in ("S", "N", " ") then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El indicador de Deuda Subordinada debe contener un valor válido (3.2.1)  R1230321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1230321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_inddeud2 not in ("S", "N", " ") then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Empresa debe existir en la tabla de baremos correspondiente (3.2.1)   R1259321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1259321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when baremo.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local  baremo on
  baremo.cod_negocio = 1 and
  baremo.cod_baremo_global = jcb.g4095_s1emp
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo País debe existir en la tabla de baremos correspondiente (3.2.1)    R1260321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1260321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when baremo.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local  baremo on
  baremo.cod_negocio = 2 and
  baremo.cod_baremo_local = lpad(jcb.g4095_id_pais, 5, '0')
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Divisa debe existir en la tabla de baremos correspondiente(3.2.1)    R1261321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1261321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when baremo.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local baremog on
    baremo.cod_negocio = 6 and
  baremo.cod_baremo_local = jcb.g4095_coddiv
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Canal Comercial debe existir en la tabla de baremos correspondiente (3.2.1)  R1262321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1262321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when baremo.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local  baremo on
  baremo.cod_negocio_local = 5 and
  baremo.cod_baremo_local = jcb.g4095_id_canal
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Canal de Contratación debe existir en la tabla de baremos correspondiente (3.2.1)   R1263321
--Ingesta
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1263321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when baremo.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local  baremo on
  baremo.cod_negocio_local = 5 and
  baremo.cod_baremo_local = jcb.g4095_id_cana2
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Naturaleza d--El Contrato debe existir en la tabla de baremos correspondiente (3.2.1)    R1264321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1264321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_id_natur <> '00000' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Naturaleza Activo Subyacente debe existir en la tabla de baremos correspondiente (3.2.1) R1265321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1265321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_id_nsuby <> '00000' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Tipo Interés debe existir en la tabla de baremos correspondiente (3.2.1)    R1266321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1266321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bglv.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local bglv on
  bglv.cod_baremo_global = jcb.g4095_tip_inte
  and bglv.cod_negocio_local = 8
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--El valor d--El campo Tipo de identificación de emisiones debe existir en la tabla de baremos correspondiente (3.2.1) R1267321
--MMFF
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1267321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bglv.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
left join bi_corp_bdr.v_map_baremos_global_local bglv on
  bglv.cod_negocio_local = 8
  and bglv.cod_baremo_global = jcb.g4095_idemis
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--
--BAREMO --> no existe código para esto
--
--El valor d--El campo Pr--Elación de la deuda debe existir en la tabla de baremos correspondiente (3.2.1) R1389321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1389321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_deudprel <> '00000' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_contr_bis')
--La fecha de entrada en vigor no debe ser anterior a la fecha de apertura d--El contrato (3.2.9)   R1455321
select
    jcb.g4095_feoperac as fecha_proceso,
    'R1455321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4095_fecentvi < jcb.g4095_fechaper then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_contr_bis jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4095_feoperac;