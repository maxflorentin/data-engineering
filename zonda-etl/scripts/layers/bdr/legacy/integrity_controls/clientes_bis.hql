Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--La fecha fin de relación debe ser mayor o igual que la fecha de inicio de relación  R0455341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0455341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchini > jcb.g4093_fchfin then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--No deben existir registros duplicados R0124341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0124341' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct g4093_idnumcli) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;
--La fecha inicio relación debe ser menor o igual que la fecha de proceso de la información   R0454341

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0454341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchini <= jcb.g4093_feoperac then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Código Sector Actividad debe existir en la tabla de baremos correspondiente 
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0506341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 9 and
  bgl.cod_baremo_global = jcb.g4093_cod_sect 
  and bgl.cod_baremo_local  = jcb.g4093_cod_sec2 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--Fecha inicio relación debe ser distinta de ‘9999-12-31’  R0757341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0757341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchini = '9999-12-31' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--Fecha inicio relación debe ser distinta de ‘0001-01-01’  R0758341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0758341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchini = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--Fecha fin relación debe ser distinta de ‘0001-01-01’ R0759341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0759341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchfin = '0001-01-01' then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El cliente debe tener algún registro asociado en la tabla Intervinientes Contrato    R0043341322
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0043341322' as cod_incidencia,
    count(distinct jcb.g4093_idnumcli) as num_reg_total,
    sum(case when intcto.g4128_idnumcli is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.jm_interv_cto intcto
    on intcto.g4128_idnumcli = jcb.g4093_idnumcli and
        intcto.g4128_feoperac = jcb.g4093_feoperac
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El identificador de cliente debe existir en la tabla Calificación cliente    R0936341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0936341' as cod_incidencia,
    count(distinct jcb.g4093_idnumcli) as num_reg_total,
    sum(case when calcli.e9954_idnumcli is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.jm_cal_in_cl calcli
    on calcli.e9954_idnumcli = jcb.g4093_idnumcli and
        calcli.e9954_feoperac = jcb.g4093_feoperac
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Industry debe existir en la tabla de baremos correspondiente   R1096341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1096341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 72 and --Verificar el BAREMO
  bgl.cod_baremo_global = jcb.g4093_industry 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--Fecha inicio relación debe ser menor o igual que Fecha de Operación R0975341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0975341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_FCHINI <= jcb.g4093_feoperac then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

--Fecha fin relación debe ser mayor o igualque Fecha de Operación     R0976341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R0976341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fchfin >= jcb.g4093_feoperac then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.4.1)    R1289341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1289341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join (select distinct bgl.cod_baremo_global from bi_corp_bdr.map_baremos_global_local bgl where bgl.cod_negocio = 1 ) bgl on --Revisar Cod. Baremos
bgl.cod_baremo_global = jcb.g4093_s1emp 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Tipo de Persona debe existir en la tabla de baremos correspondiente (3.4.1)    R1290341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1290341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 23 and
  bgl.cod_baremo_global = jcb.G4093_TIP_PERS 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Marca de Carterizado debe existir en la tabla de baremos correspondiente (3.4.1)   R1291341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1291341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 39 and
  bgl.cod_baremo_global = jcb.g4093_carter 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo País debe existir en la tabla de baremos correspondiente (3.4.1)  R1292341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1292341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 2 and
  bgl.cod_baremo_global = jcb.g4093_id_pais 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo Código de Segmento Cliente debe existir en la tabla de baremos correspondiente (3.4.1)    R1293341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1293341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 56 and -- no hay baremos
  bgl.cod_baremo_global = jcb.g4093_clisegm 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--La Fecha de Nacimiento debe ser menor o igual que la fecha de proceso de los datos (3.4.1)    R1456341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1456341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_fecnacim <= jcb.g4093_feoperac then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo País de negocio debe existir en la tabla de baremos correspondiente (3.4.1)   R1457341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1457341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 2 and
  bgl.cod_baremo_global = jcb.g4093_paisneg 
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo tratamiento especial no existe (3.4.10)    R1563341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1563341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_tto_espe is null or length(trim(jcb.g4093_tto_espe)) = 0 then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo grado de vinculación no existe (3.4.10)   R1564341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1564341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_gra_vinc is null or length(trim(jcb.g4093_gra_vinc)) = 0 then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;

Insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_client_bii')
--El valor del campo UTP Cliente no existe (3.4.10) R1565341
select
    jcb.g4093_feoperac as fecha_proceso,
    'R1565341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jcb.g4093_utp_cli is null or length(trim(jcb.g4093_utp_cli)) = 0 then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_client_bii jcb
where jcb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='EXEC_BDR_INT_TST-New_Default-Monthly') }}'
group by jcb.g4093_feoperac;