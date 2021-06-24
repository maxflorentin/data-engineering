set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmoferta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
substr(fec_proceso,1,19) as ts_adm_fecproceso,
label as ds_adm_variable,
cast(b.AsistenciaMaximaCliente as double) as fc_adm_asistenciamaximacliente,
b.codPromocion as cod_adm_codpromocion,
b.codigoAmex as cod_adm_codigoamex,
b.codigoMaster as cod_adm_codigomaster,
b.codigoVisa as cod_adm_codigovisa,
b.codigoVisaBusiness as cod_adm_codigovisabusiness,
cast(b.disponibleAcc as double) as fc_adm_disponibleacc,
cast(b.disponibleAmex as double) as fc_adm_disponibleamex,
cast(b.disponibleMaster as double) as fc_adm_disponiblemaster,
cast(b.disponiblePPP as double) as fc_adm_disponibleppp,
cast(b.disponibleSC1 as double) as fc_adm_disponiblesc1,
cast(b.disponibleVisa as double) as fc_adm_disponiblevisa,
cast(b.disponibleVisaBusiness as double) as fc_adm_disponiblevisabusiness,
cast(b.limiteMaxAcc as double) as fc_adm_limitemaxacc,
cast(b.limiteMaxAmex as double) as fc_adm_limitemaxamex,
cast(b.limiteMaxMaster as double) as fc_adm_limitemaxmaster,
cast(b.limiteMaxPPP as double) as fc_adm_limitemaxppp,
cast(b.limiteMaxSC1 as double) as fc_adm_limitemaxsc1,
cast(b.limiteMaxVisa as double) as fc_adm_limitemaxvisa,
cast(b.limiteMaxVisaBusiness as double) as fc_adm_limitemaxvisabusiness,
cast(b.tomadoAcc as double) as fc_adm_tomadoacc,
cast(b.tomadoAmex as double) as fc_adm_tomadoamex,
cast(b.tomadoMaster as double) as fc_adm_tomadomaster,
cast(b.tomadoPPP as double) as fc_adm_tomadoppp,
cast(b.tomadoSC1 as double) as fc_adm_tomadosc1,
cast(b.tomadoVisa as double) as fc_adm_tomadovisa,
cast(b.tomadoVisaBusiness as double) as fc_adm_tomadovisabusiness,
b.codPaquete as cod_adm_codpaquete,
json as ds_adm_json
from bi_corp_staging.scoring_omdm_solicitudes
lateral view json_tuple(json,
'AsistenciaMaximaCliente',
'codPromocion',
'codigoAmex',
'codigoMaster',
'codigoVisa',
'codigoVisaBusiness',
'disponibleAcc',
'disponibleAmex',
'disponibleMaster',
'disponiblePPP',
'disponibleSC1',
'disponibleVisa',
'disponibleVisaBusiness',
'limiteMaxAcc',
'limiteMaxAmex',
'limiteMaxMaster',
'limiteMaxPPP',
'limiteMaxSC1',
'limiteMaxVisa',
'limiteMaxVisaBusiness',
'tomadoAcc',
'tomadoAmex',
'tomadoMaster',
'tomadoPPP',
'tomadoSC1',
'tomadoVisa',
'tomadoVisaBusiness',
'codPaquete'
) b as AsistenciaMaximaCliente,codPromocion,codigoAmex,codigoMaster,codigoVisa,codigoVisaBusiness,disponibleAcc,disponibleAmex,disponibleMaster,disponiblePPP,disponibleSC1,disponibleVisa,disponibleVisaBusiness,limiteMaxAcc,limiteMaxAmex,limiteMaxMaster,limiteMaxPPP,limiteMaxSC1,limiteMaxVisa,limiteMaxVisaBusiness,tomadoAcc,tomadoAmex,tomadoMaster,tomadoPPP,tomadoSC1,tomadoVisa,tomadoVisaBusiness,codPaquete
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label like 'Oferta%';