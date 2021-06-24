set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes_propuesta_pyme
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;


DROP TABLE IF EXISTS balance;
CREATE TEMPORARY TABLE balance AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes_propuesta_pyme omdm
lateral view json_tuple(json,
'activoCorriente',
'activoCorrienteAnterior',
'activoTotal',
'activoTotalAnterior',
'amortizaciones',
'amortizacionesAnterior',
'anticipoAccionistas',
'anticipoAccionistasAnterior',
'bienesCambio',
'bienesCambioAnterior',
'bienesUso',
'bienesUsoAnterior',
'cantidadMesesDesdeBceCargado',
'cantidadMesesDesdeBceCargadoAnterior',
'compras',
'comprasAnterior',
'costoVentas',
'costoVentasAnterior',
'creditosComerciales',
'creditosComercialesAnterior',
'deudasComercialesCP',
'deudasComercialesCPAnterior',
'deudasFinancierasCP',
'deudasFinancierasCPAnterior',
'deudasFinancierasLP',
'deudasFinancierasLPAnterior',
'deudasFiscalesCP',
'deudasFiscalesCPAnterior',
'deudasFiscalesLP',
'deudasFiscalesLPAnterior',
'disponibilidades',
'disponibilidadesAnterior',
'dividendosRetirados',
'dividendosRetiradosAnterior',
'fechaCierreBalance',
'fechaCierreBalanceAnterior',
'gastosAdministrativos',
'gastosAdministrativosAnterior',
'gastosComerciales',
'gastosComercialesAnterior',
'gastosFinanciacion',
'gastosFinanciacionAnterior',
'impuestosGanancias',
'impuestosGananciasAnterior',
'otrosPasivosLP',
'otrosPasivosLPAnterior',
'pasivoCorriente',
'pasivoCorrienteAnterior',
'pasivoTotal',
'pasivoTotalAnterior',
'patrimonioNeto',
'patrimonioNetoAnterior',
'resultadoExtraordinario',
'resultadoExtraordinarioAnterior',
'resultadoFinal',
'resultadoFinalAnterior',
'resultadoOperativo',
'resultadoOperativoAnterior',
'tipoBalance',
'ventas',
'ventasAnterior'
) b as activoCorriente, activoCorrienteAnterior, activoTotal, activoTotalAnterior, amortizaciones, amortizacionesAnterior, anticipoAccionistas,anticipoAccionistasAnterior,bienesCambio,bienesCambioAnterior,bienesUso,bienesUsoAnterior,cantidadMesesDesdeBceCargado,cantidadMesesDesdeBceCargadoAnterior,compras,comprasAnterior,costoVentas,costoVentasAnterior,creditosComerciales,creditosComercialesAnterior,deudasComercialesCP,deudasComercialesCPAnterior,deudasFinancierasCP,deudasFinancierasCPAnterior,deudasFinancierasLP,deudasFinancierasLPAnterior,deudasFiscalesCP,deudasFiscalesCPAnterior,deudasFiscalesLP,deudasFiscalesLPAnterior,disponibilidades,disponibilidadesAnterior,dividendosRetirados,dividendosRetiradosAnterior,fechaCierreBalance,fechaCierreBalanceAnterior,gastosAdministrativos,gastosAdministrativosAnterior,gastosComerciales,gastosComercialesAnterior,gastosFinanciacion,gastosFinanciacionAnterior,impuestosGanancias,impuestosGananciasAnterior,otrosPasivosLP,otrosPasivosLPAnterior,pasivoCorriente,pasivoCorrienteAnterior,pasivoTotal,pasivoTotalAnterior,patrimonioNeto,patrimonioNetoAnterior,resultadoExtraordinario,resultadoExtraordinarioAnterior,resultadoFinal,resultadoFinalAnterior,resultadoOperativo,resultadoOperativoAnterior,tipoBalance,ventas,ventasAnterior
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Balance';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_omdmbalance
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select distinct
omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
cast(omdm.activoCorriente as double) as fc_adm_activocorriente,
cast(omdm.activoCorrienteAnterior as double) as fc_adm_activocorrienteanterior,
cast(omdm.activoTotal as double) as fc_adm_activototal,
cast(omdm.activoTotalAnterior as double) as fc_adm_activototalanterior,
cast(omdm.amortizaciones as double) as fc_adm_amortizaciones,
cast(omdm.amortizacionesAnterior as double) as fc_adm_amortizacionesanterior,
cast(omdm.anticipoAccionistas as double) as fc_adm_anticipoaccionistas,
cast(omdm.anticipoAccionistasAnterior as double) as fc_adm_anticipoaccionistasanterior,
cast(omdm.bienesCambio as double) as fc_adm_bienescambio,
cast(omdm.bienesCambioAnterior as double) as fc_adm_bienescambioanterior,
cast(omdm.bienesUso as double) as fc_adm_bienesuso,
cast(omdm.bienesUsoAnterior as double) as fc_adm_bienesusoanterior,
cast(omdm.cantidadMesesDesdeBceCargado as int) as int_adm_cantidadmesesdesdebcecargado,
cast(omdm.cantidadMesesDesdeBceCargadoAnterior as int) as int_adm_cantidadmesesdesdebcecargadoanterior,
cast(omdm.compras as double) as fc_adm_compras,
cast(omdm.comprasAnterior as double) as fc_adm_comprasanterior,
cast(omdm.costoVentas as double) as fc_adm_costoventas,
cast(omdm.costoVentasAnterior as double) as fc_adm_costoventasanterior,
cast(omdm.creditosComerciales as double) as fc_adm_creditoscomerciales,
cast(omdm.creditosComercialesAnterior as double) as fc_adm_creditoscomercialesanterior,
cast(omdm.deudasComercialesCP as double) as fc_adm_deudascomercialescp,
cast(omdm.deudasComercialesCPAnterior as double) as fc_adm_deudascomercialescpanterior,
cast(omdm.deudasFinancierasCP as double) as fc_adm_deudasfinancierascp,
cast(omdm.deudasFinancierasCPAnterior as double) as fc_adm_deudasfinancierascpanterior,
cast(omdm.deudasFinancierasLP as double) as fc_adm_deudasfinancieraslp,
cast(omdm.deudasFinancierasLPAnterior as double) as fc_adm_deudasfinancieraslpanterior,
cast(omdm.deudasFiscalesCP as double) as fc_adm_deudasfiscalescp,
cast(omdm.deudasFiscalesCPAnterior as double) as fc_adm_deudasfiscalescpanterior,
cast(omdm.deudasFiscalesLP as double) as fc_adm_deudasfiscaleslp,
cast(omdm.deudasFiscalesLPAnterior as double) as fc_adm_deudasfiscaleslpanterior,
cast(omdm.disponibilidades as double) as fc_adm_disponibilidades,
cast(omdm.disponibilidadesAnterior as double) as fc_adm_disponibilidadesanterior,
cast(omdm.dividendosRetirados as double) as fc_adm_dividendosretirados,
cast(omdm.dividendosRetiradosAnterior as double) as fc_adm_dividendosretiradosanterior,
omdm.fechaCierreBalance as dt_adm_fechacierrebalance,
omdm.fechaCierreBalanceAnterior as dt_adm_fechacierrebalanceanterior,
cast(omdm.gastosAdministrativos as double) as fc_adm_gastosadministrativos,
cast(omdm.gastosAdministrativosAnterior as double) as fc_adm_gastosadministrativosanterior,
cast(omdm.gastosComerciales as double) as fc_adm_gastoscomerciales,
cast(omdm.gastosComercialesAnterior as double) as fc_adm_gastoscomercialesanterior,
cast(omdm.gastosFinanciacion as double) as fc_adm_gastosfinanciacion,
cast(omdm.gastosFinanciacionAnterior as double) as fc_adm_gastosfinanciacionanterior,
cast(omdm.impuestosGanancias as double) as fc_adm_impuestosganancias,
cast(omdm.impuestosGananciasAnterior as double) as fc_adm_impuestosgananciasanterior,
cast(omdm.otrosPasivosLP as double) as fc_adm_otrospasivoslp,
cast(omdm.otrosPasivosLPAnterior as double) as fc_adm_otrospasivoslpanterior,
cast(omdm.pasivoCorriente as double) as fc_adm_pasivocorriente,
cast(omdm.pasivoCorrienteAnterior as double) as fc_adm_pasivocorrienteanterior,
cast(omdm.pasivoTotal as double) as fc_adm_pasivototal,
cast(omdm.pasivoTotalAnterior as double) as fc_adm_pasivototalanterior,
cast(omdm.patrimonioNeto as double) as fc_adm_patrimonioneto,
cast(omdm.patrimonioNetoAnterior as double) as fc_adm_patrimonionetoanterior,
cast(omdm.resultadoExtraordinario as double) as fc_adm_resultadoextraordinario,
cast(omdm.resultadoExtraordinarioAnterior as double) as fc_adm_resultadoextraordinarioanterior,
cast(omdm.resultadoFinal as double) as fc_adm_resultadofinal,
cast(omdm.resultadoFinalAnterior as double) as fc_adm_resultadofinalanterior,
cast(omdm.resultadoOperativo as double) as fc_adm_resultadooperativo,
cast(omdm.resultadoOperativoAnterior as double) as fc_adm_resultadooperativoanterior,
cast(omdm.tipoBalance as int) as cod_adm_tipobalance,
cast(omdm.ventas as double) as fc_adm_ventas,
cast(omdm.ventasAnterior as double) as fc_adm_ventasanterior,
omdm.json as ds_adm_json
from balance omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;