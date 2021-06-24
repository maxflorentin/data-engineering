"set mapred.job.queue.name=root.dataeng;
--STOCK
select cli.ascl_nu_nup nup,
cli.ascl_tp_documento tipo_doc,
cli.ascl_nu_documento nro_doc,
cli.ascl_apellido apellido,
cli.ascl_nombre nombre,
c.asct_cd_ramo ramo ,
c.asct_nu_contrato contrato,
c.asct_nu_certificado certificado,
c.asct_fe_contratacion fecha_contratacion,
c.asct_cd_producto cod_producto,
p.aspr_de_producto desc_producto,
c.asct_cd_plan cod_plan,
pl.aspl_de_plan desc_plan,
c.asct_mt_cuota monto_cuota,
c.asct_mt_comision monto_comision,
c.asct_cd_sucursal sucursal,
c.asct_st_certificado estado_certificado,
cast(null as string) AS cod_operacion,
cast(null as string) AS fecha_operacion,
cast(null as string) AS legajo,
'S' tipo_operacion,
c.partition_date
FROM bi_corp_staging.rio6_asit_contratos c
LEFT JOIN bi_corp_staging.rio6_asit_productos p
ON (c.asct_cd_producto = p.aspr_cd_producto AND
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
    )
LEFT JOIN bi_corp_staging.rio6_asit_planes pl
ON (c.asct_cd_plan= pl.aspl_cd_plan  AND
     pl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
   )
LEFT JOIN bi_corp_staging.rio6_asit_clientes cli
ON (c.asct_tp_documento = cli.ASCL_TP_DOCUMENTO AND
    c.asct_nu_documento = cli.ASCL_NU_DOCUMENTO AND
    cli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
    )
WHERE    c.asct_st_certificado = '1'
AND      c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
UNION ALL
--ALTAS Y BAJAS
select cli.ascl_nu_nup nup,
cli.ascl_tp_documento tipo_doc,
cli.ascl_nu_documento nro_doc,
cli.ascl_apellido apellido,
cli.ascl_nombre nombre,
c.asct_cd_ramo ramo ,
c.asct_nu_contrato contrato,
c.asct_nu_certificado certificado,
c.asct_fe_contratacion fecha_contratacion,
c.asct_cd_producto cod_producto,
p.aspr_de_producto desc_producto,
c.asct_cd_plan cod_plan,
pl.aspl_de_plan desc_plan,
c.asct_mt_cuota monto_cuota,
c.asct_mt_comision monto_comision,
c.asct_cd_sucursal sucursal,
c.asct_st_certificado estado_certificado,
OC.ASOC_CD_OPERACION cod_operacion,
oc.asoc_fe_operacion fecha_operacion,
oc.asoc_cd_usuario legajo,
CASE WHEN oc.asoc_cd_operacion = '1001' THEN 'A'
           ELSE 'B'
END tipo_operacion,
oc.partition_date
FROM bi_corp_staging.rio6_asit_operaciones_contratos oc
LEFT JOIN bi_corp_staging.rio6_asit_contratos c
ON (oc.asoc_cd_ramo        = c.asct_cd_ramo AND
    OC.ASOC_NU_CONTRATO    = c.asct_nu_contrato   AND
    OC.ASOC_NU_CERTIFICADO = c.asct_nu_certificado AND
    c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
    )
LEFT JOIN bi_corp_staging.rio6_asit_productos p
ON (c.asct_cd_producto = p.aspr_cd_producto AND
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
    )
LEFT JOIN bi_corp_staging.rio6_asit_planes pl
ON (c.asct_cd_plan= pl.aspl_cd_plan AND
    pl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
   )
LEFT JOIN bi_corp_staging.rio6_asit_clientes cli
ON (c.asct_tp_documento = cli.ASCL_TP_DOCUMENTO AND
    c.asct_nu_documento = cli.ASCL_NU_DOCUMENTO AND
    cli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
    )
WHERE oc.asoc_cd_operacion IN ( '1001','1003') --Alta/Baja de asistencias
AND   oc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ASISTENCIAS_REPORT_RIO53') }}'
"