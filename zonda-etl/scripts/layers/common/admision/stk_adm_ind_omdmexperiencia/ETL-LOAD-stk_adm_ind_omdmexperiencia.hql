set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS experiencia;
CREATE TEMPORARY TABLE experiencia AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
label,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'cantAdelantosEnEfectivoDolares',
'cantAdelantosEnEfectivoPesos',
'monAdelantosEnEfectivoDolares',
'monAdelantosEnEfectivoPesos',
'rentaCliente',
'tipoCliente',
'tipoPaquetePosee',
'cantCheqRechSinFondoNoResc',
'cantCheqSinFondoResc',
'imporCheqSinFondoNoResc',
'imporCheqSinFondoResc',
'cantAtr30Ult12',
'cantAtr60Ult12',
'cantAtr90Ult12',
'cantAtrM90Ult12',
'fechaAlta',
'limiteDeAcuerdo',
'mayorMontoDelExceso',
'montoDelExceso',
'plazoMora',
'tipoCuenta',
'in00Causal1',
'in00FechaInicio1',
'in00FechaVto1',
'cantDiasDeAtrasos',
'esPreacordadoMonoproducto',
'fechaBaja',
'importeUltimaCuota',
'modalidad',
'saldoImpago',
'tipoMonoproducto',
'tipoPrestamo',
'atrasos',
'estado',
'fechaBoletin',
'marca',
'montoUltimoPagoMinimo',
'montoUltimoResumen',
'motivoBaja',
'tipoTarjeta',
'marcaBloqueoSeguimiento',
'marcaCuotaPhoneMoroso'
) b as cantAdelantosEnEfectivoDolares,cantAdelantosEnEfectivoPesos,monAdelantosEnEfectivoDolares,monAdelantosEnEfectivoPesos,rentaCliente,tipoCliente,tipoPaquetePosee,cantCheqRechSinFondoNoResc,cantCheqSinFondoResc,imporCheqSinFondoNoResc,imporCheqSinFondoResc,cantAtr30Ult12,cantAtr60Ult12,cantAtr90Ult12,cantAtrM90Ult12,fechaAlta,limiteDeAcuerdo,mayorMontoDelExceso,montoDelExceso,plazoMora,tipoCuenta,in00Causal1,in00FechaInicio1,in00FechaVto1,cantDiasDeAtrasos,esPreacordadoMonoproducto,fechaBaja,importeUltimaCuota,modalidad,saldoImpago,tipoMonoproducto,tipoPrestamo,atrasos,estado,fechaBoletin,marca,montoUltimoPagoMinimo,montoUltimoResumen,motivoBaja,tipoTarjeta,marcaBloqueoSeguimiento,marcaCuotaPhoneMoroso
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label like 'Experiencia%';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmexperiencia
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.label as ds_adm_variable,
cast(omdm.cantAdelantosEnEfectivoDolares as int) as int_adm_cantadelantosenefectivodolares,
cast(omdm.cantAdelantosEnEfectivoPesos as int) as int_adm_cantadelantosenefectivopesos,
cast(omdm.monAdelantosEnEfectivoDolares as double) as fc_adm_monadelantosenefectivodolares,
cast(omdm.monAdelantosEnEfectivoPesos as double) as fc_adm_monadelantosenefectivopesos,
omdm.rentaCliente as ds_adm_rentacliente,
omdm.tipoCliente as ds_adm_tipocliente,
omdm.tipoPaquetePosee as ds_adm_tipopaqueteposee,
cast(omdm.cantCheqRechSinFondoNoResc as int) as int_adm_cantcheqrechsinfondonoresc,
cast(omdm.cantCheqSinFondoResc as int) as int_adm_cantcheqsinfondoresc,
cast(omdm.imporCheqSinFondoNoResc as double) as fc_adm_imporcheqsinfondonoresc,
cast(omdm.imporCheqSinFondoResc as double) as fc_adm_imporcheqsinfondoresc,
cast(omdm.cantAtr30Ult12 as int) as int_adm_cantatr30ult12,
cast(omdm.cantAtr60Ult12 as int) as int_adm_cantatr60ult12,
cast(omdm.cantAtr90Ult12 as int) as int_adm_cantatr90ult12,
cast(omdm.cantAtrM90Ult12 as int) as int_adm_cantatrm90ult12,
omdm.fechaAlta as dt_adm_fechaalta,
cast(omdm.limiteDeAcuerdo as double) as fc_adm_limitedeacuerdo,
cast(omdm.mayorMontoDelExceso as double) as fc_adm_mayormontodelexceso,
cast(omdm.montoDelExceso as double) as fc_adm_montodelexceso,
cast(omdm.plazoMora as int) as int_adm_plazomora,
omdm.tipoCuenta as ds_adm_tipocuenta,
omdm.in00Causal1 as cod_adm_in00causal1,
omdm.in00FechaInicio1 as dt_adm_in00fechainicio1,
omdm.in00FechaVto1 as dt_adm_in00fechavto1,
cast(omdm.cantDiasDeAtrasos as int) as int_adm_cantdiasdeatrasos,
omdm.esPreacordadoMonoproducto as flag_adm_espreacordadomonoproducto,
omdm.fechaBaja as dt_adm_fechabaja,
cast(omdm.importeUltimaCuota as double) as fc_adm_importeultimacuota,
omdm.modalidad as cod_adm_modalidad,
cast(omdm.saldoImpago as double) as fc_adm_saldoimpago,
omdm.tipoMonoproducto as cod_adm_tipomonoproducto,
omdm.tipoPrestamo as cod_adm_tipoprestamo,
cast(omdm.atrasos as int) as cod_adm_atrasos,
omdm.estado as cod_adm_estado,
omdm.fechaBoletin as dt_adm_fechaboletin,
omdm.marca as ds_adm_marca,
cast(omdm.montoUltimoPagoMinimo as double) as fc_adm_montoultimopagominimo,
cast(omdm.montoUltimoResumen as double) as fc_adm_montoultimoresumen,
omdm.motivoBaja as cod_adm_motivobaja,
omdm.tipoTarjeta as ds_adm_tipotarjeta,
omdm.marcaBloqueoSeguimiento as cod_adm_marcabloqueoseguimiento,
omdm.marcaCuotaPhoneMoroso as flag_adm_marcacuotaphonemoroso,
omdm.json as ds_adm_json
from experiencia omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
