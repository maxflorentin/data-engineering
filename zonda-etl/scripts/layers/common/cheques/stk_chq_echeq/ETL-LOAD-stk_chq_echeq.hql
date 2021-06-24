set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS pedt008_use_echeq;
CREATE TEMPORARY TABLE pedt008_use_echeq AS
select p.pecdgent, p.pecodofi, p.penumcon, p.peordpar, p.penumper
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, max(PEORDPAR) peordpar
from
(select pecodofi, penumcon, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and pecalpar = 'TI'
and pecodpro in ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
and peestrel = 'A'
group by pecdgent, PECODOFI,penumcon

union all

select pecodofi, penumcon, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and pecodpro in ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
and pecalpar = 'TI'
group by PECODOFI,penumcon) t1
group by pecodofi, penumcon) t2 on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and  p.pecalpar = 'TI'
and p.pecodpro in ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
group by p.pecdgent, p.pecodofi, p.penumcon, p.peordpar, p.penumper;

DROP TABLE IF EXISTS pedt015_use_echeq;
CREATE TEMPORARY TABLE pedt015_use_echeq AS
select penumdoc as cuit, max(cast(regexp_replace(penumper, "^0+", '') as bigint)) penumper
from bi_corp_staging.malpe_pedt015 p15
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
and cast(regexp_replace(penumdoc, "^0+", '') as bigint) > 20000000000
and trim(p15.petipdoc) in ('L','D','T')
group by penumdoc;

insert overwrite table bi_corp_common.stk_chq_echeq
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')

SELECT
case when ct_em_banco_codigo in ('null','NULL') then lpad(substr(cmc7,1,3), 4, '0') else ct_em_banco_codigo end as cod_chq_entidad ,
case when cheque_numero in ('null','NULL') then cast(substr(cmc7,11,8) as bigint) else cast(cheque_numero as bigint) end as cod_chq_nrocheque ,
case when ct_em_emisor_cuenta  in ('null','NULL') then cast(substr(cmc7,23,11) as bigint) else cast(ct_em_emisor_cuenta as bigint) end as cod_chq_cuentacheque ,
case when ct_em_sucursal_codigo in ('null','NULL') then cast(lpad(substr(cmc7,4,3), 4, '0') as bigint) else cast(ct_em_sucursal_codigo as bigint) end as cod_chq_sucursalgirada,
case when ct_em_sucursal_cp  in ('null','NULL') then cast(substr(cmc7,7,4) as int) else cast(ct_em_sucursal_cp as int) end as cod_chq_codigopostal,
case when ct_em_emisor_subcuenta in ('null','NULL')  then cast(null as bigint) else cast(concat('0',ct_em_emisor_subcuenta) as bigint) end as cod_chq_cuentacorriente,
case when ct_em_banco_nombre in ('null','NULL') then cast(null as string) else ct_em_banco_nombre end as ds_chq_bancoemisor,
case when ct_em_sucursal_nombre in ('null','NULL') then cast(null as string) else ct_em_sucursal_nombre end as ds_chq_sucursalbancoemisor,
case when ct_em_sucursal_domicilio in ('null','NULL') then cast(null as string) else ct_em_sucursal_domicilio end as ds_chq_domiciliosucursalbancoemisor,
case when ct_em_sucursal_provincia in ('null','NULL') then cast(null as string) else ct_em_sucursal_provincia end as ds_chq_provinciasucursalbancoemisor,
case when ct_em_emisor_cuit in ('null','NULL') then cast(null as string) else ct_em_emisor_cuit end as ds_chq_cuitemisor,
case when upper(regexp_replace(ct_em_emisor_razon_social,' +',' ')) in ('null','NULL') then cast(null as string) else upper(regexp_replace(ct_em_emisor_razon_social,' +',' ')) end as ds_chq_razonsocialemisor,
case when ct_em_emisor_cbu in ('null','NULL') then cast(null as string) else ct_em_emisor_cbu end as cod_chq_cbuemisor,
case when ct_em_emisor_subcuenta in ('null','NULL') then cast(null as string) else ct_em_emisor_subcuenta end as cod_chq_nrosubcuentaemisor,
case when trim(ct_em_emisor_moneda)='032' then 'ARS' else cast(null as string) end as cod_chq_divisa,
case when ct_em_emisor_domicilio in ('null','NULL') then cast(null as string) else ct_em_emisor_domicilio end as ds_chq_domicilioemisor,
case when ct_em_emisor_cp in ('null','NULL') then cast(null as string) else ct_em_emisor_cp end as ds_chq_cpdomicilioemisor,
case when numero_chequera in ('null','NULL') then cast(null as string) else numero_chequera end as cod_chq_numchequera,
case when cmc7 in ('null','NULL') then null else cmc7 end as cod_chq_cmc7,
case when estado in ('null','NULL') then cast(null as string) else estado end as ds_chq_estado,
case when emitido_a_benef_doc_tipo in ('null','NULL') then cast(null as string) else upper(emitido_a_benef_doc_tipo) end as cod_chq_tipodocemitidoa,
case when emitido_a_benef_doc in ('null','NULL') then cast(null as string) else emitido_a_benef_doc end as cod_chq_docemitidoa,
case when emitido_a_benef_nombre in ('null','NULL') then cast(null as string) else emitido_a_benef_nombre end as ds_chq_nombreemitidoa,
case when tenencia_benef_doc_tipo in ('null','NULL') then cast(null as string) else tenencia_benef_doc_tipo end as cod_chq_tipodoctenencia,
case when tenencia_benef_doc in ('null','NULL') then cast(null as string) else tenencia_benef_doc end as cod_chq_doctenencia,
case when tenencia_benef_nombre in ('null','NULL') then cast(null as string) else tenencia_benef_nombre end as ds_chq_nombretenencia,
case when TO_DATE(fecha_pago) in ('9999-12-31', '0001-01-01') then cast(null as string) else TO_DATE(fecha_pago) end as dt_chq_fechapago,
substring(fecha_emision, 1, 10) as ts_chq_fechaemision,
case when cheque_tipo in ('null','NULL') then cast(null as string) else cheque_tipo end as cod_chq_tipo,
case when cheque_caracter in ('null','NULL') then cast(null as string) else upper(cheque_caracter) end as ds_chq_caractercheque,
case when cheque_modo in ('null','NULL') then cast(null as string) else cheque_modo end as ds_chq_modocheque,
case when cheque_concepto in ('null','NULL') then cast(null as string) else cheque_concepto end as ds_chq_conceptocheque,
case when cheque_motivo_pago in ('null','NULL') then cast(null as string) else upper(cheque_motivo_pago) end as ds_chq_motivopagocheque,
case when upper(motivo_repudio_emision) in ('null','NULL') then cast(null as string) else upper(motivo_repudio_emision)  end as ds_chq_motivorepudioemision,
case when motivo_anulacion in ('null','NULL') then cast(null as string) else upper(motivo_anulacion) end as ds_chq_motivoanulacion,
cast(fecha_pago_vencida as int) as flag_chq_fechapagovencida,
case when cbu_custodia in ('null','NULL') then cast(null as string) else cbu_custodia end as ds_chq_cbucustodia,
case when cbu_deposito in ('null','NULL') then cast(null as string) else cbu_deposito end as ds_chq_cbudeposito,
cast(cheque_acordado as int)  as flag_chq_chequeacordado,
cast(solicitando_acuerdo as int) as flag_chq_solicitandoacuerdo,
cast(re_presentar as int) as flag_chq_representar,
cast(repudio_endoso as int) as flag_chq_repudioendoso,
cast(certificado_emitido as int) as flag_chq_certificadoemitido,
cast(acuerdo_rechazado as int) as flag_chq_acuerdorechazado,
cast(coalesce(cast(p1.penumper as bigint),p15.penumper) as bigint) as cod_per_nup,
p30.pesegcla as cod_chq_segmento,
p30.pesegcal as cod_chq_agrupacionsegmento,
cast(null as string) as cod_chq_subsegmento,
p30.peclaseg as cod_chq_clasesegmento,
case when c.operation_id in ('null','NULL') then cast(null as string) else c.operation_id end as cod_chq_nrooperacion,
cast(monto as decimal(19, 4)) as fc_chq_monto
FROM bi_corp_staging.echeq_cheques c
left join pedt008_use_echeq p1 on p1.pecdgent = '0072' and c.ct_em_sucursal_codigo = p1.PECODOFI and (case when c.ct_em_emisor_subcuenta='00000000000' then SUBSTR (c.ct_em_emisor_cuenta, -8) else SUBSTR (c.ct_em_emisor_subcuenta, -8) end) = SUBSTR (p1.penumcon, -8)
left join pedt015_use_echeq p15 on p15.cuit=c.ct_em_emisor_cuit
LEFT JOIN bi_corp_staging.malpe_pedt030 p30 on p30.penumper=p1.penumper and p30.partition_date = c.updated
--LEFT JOIN bi_corp_risk.segmentos s on s.segmento=trim(p30.pesegcla)
WHERE updated = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
;