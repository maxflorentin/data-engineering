"
SET mapred.job.queue.name=root.dataeng;
SELECT B.PETIPDOC,
       B.PENUMDOC,
       concat(trim(B.PENOMPER), ' ', trim(B.PEPRIAPE)),
       B.PETIPPER,
       B.PESUCADM,
       B.PECONPER,
        DATE_FORMAT (B.PEFECNAC, 'yyyyMMdd'),
        DATE_FORMAT (B.PEFECINI, 'yyyyMMdd'),
       concat(trim(F.PENOMCAL), ' ',trim(F.PENUMBLO)),
       F.PEDESLOC,
       F.PECODPOS,
       B.PEPAIRES,
       B.PEPAIORI,
       B.PENACPER,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}',
       'GAR',
       concat(B.PENUMPER, ' ',cast(min(C.gttcrgb_rgb_num_garantia) as string))
   FROM  bi_corp_staging.gtdtrbc   A,
                bi_corp_staging.malpe_pedt001 B,
                bi_corp_staging.gtdtrgb  C,
                bi_corp_staging.GTDTMAE  D,
                bi_corp_staging.gtdtgar  E,
                bi_corp_staging.malpe_pedt003   F
WHERE A.gttcrbc_rbc_cod_entidad  = '0072'
AND   A.gttcrbc_rbc_cod_entidad  = B.PECDGENT
AND   A.gttcrbc_rbc_num_persona  = B.PENUMPER
AND   A.gttcrbc_rbc_cod_entidad  = C.gttcrgb_rgb_cod_entidad
AND   a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   B.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   C.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   D.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   E.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   F.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   F.PECDGENT     = '0072'
AND   B.PENUMPER     = F.PENUMPER
AND   trim(F.PESECDOM)     = '001'
AND   A.gttcrbc_rbc_num_bien     = C.gttcrgb_rgb_num_bien
AND   C.gttcrgb_rgb_cod_entidad  = D.gttcmae_mae_cod_entidad
AND   C.gttcrgb_rgb_num_garantia = D.gttcmae_mae_num_garantia
AND   D.gttcmae_mae_cod_entidad  = E.gttcgar_gar_cod_entidad
AND   D.gttcmae_mae_cod_garantia = E.gttcgar_gar_cod_garantia
AND   B.PECONPER    <> 'CLI'
AND   D.gttcmae_mae_cod_estado   = '020'
AND   A.gttcrbc_rbc_fec_finvali > '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   C.gttcrgb_rgb_fec_finvali > '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   D.gttcmae_mae_fec_vcto    > '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NORKOM_GTDTRBC') }}'
AND   E.gttcgar_gar_cla_garantia = '003'
group by B.PETIPDOC,
         B.PENUMDOC,
        concat(trim(B.PENOMPER), ' ', trim(B.PEPRIAPE)),
        B.PETIPPER,
        B.PESUCADM,
        B.PECONPER,
        B.PEFECNAC,
        B.PEFECINI,
        concat(trim(F.PENOMCAL), ' ',trim(F.PENUMBLO)),
        F.PEDESLOC,
        F.PECODPOS,
        B.PEPAIRES,
        B.PEPAIORI,
        B.PENACPER,
        B.PENUMPER
"