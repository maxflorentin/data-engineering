set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS participantes;
CREATE TEMPORARY TABLE participantes AS
select tipo_tramite, cod_tramite, participante, b.tipoParticipante as tipo_participante
from bi_corp_staging.scoring_omdm_participantes
lateral view json_tuple(json,'tipoParticipante') b as tipoParticipante
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'Participante'
group by tipo_tramite, cod_tramite, participante, b.tipoParticipante;

DROP TABLE IF EXISTS datosfinancieros;
CREATE TEMPORARY TABLE datosfinancieros AS
select tipo_tramite,
cod_tramite,
participante,
fec_proceso as fec_proceso,
label,
b.*,
json
from bi_corp_staging.scoring_omdm_participantes omdm
lateral view json_tuple(json,
'antiguedadCuentaActMasAnt',
'antiguedadPrestHipotecarioActMasAnt',
'antiguedadPrestPersonalActMasAnt',
'antiguedadPrestPrendarioActMasAnt',
'antiguedadTotalBSR',
'cantidadMesesInversionesTotal',
'cantidadMesesMBB',
'cantidadMesesSaldoCuenta',
'cuadranteCRM',
'montoCuotaTotalPrestHipotecario',
'montoCuotaTotalPrestPersonal',
'montoCuotaTotalPrestPrendario',
'montoMBBPromedio6M',
'montoMinimoInversionesTotal6M',
'montoMinimoMBBU6M',
'montoTenenciaAcciones',
'montoTenenciaBono',
'montoTenenciaPlazoFijo',
'promedioConsumosDebito6M',
'promedioConsumosTarjAmex6M',
'promedioConsumosTarjMaster6M',
'promedioConsumosTarjVisa6M',
'promedioExtraccionesDebito6M',
'promedioInversionesTotal6M',
'promedioSaldoCuenta6M',
'saldoDeudaAPTP',
'saldoDeudaPrestamoMono',
'tasa60MPPmono',
'consumo',
'limite',
'codTipoTarjeta',
'saldoNoUtilizado'
) b as antiguedadCuentaActMasAnt,antiguedadPrestHipotecarioActMasAnt,antiguedadPrestPersonalActMasAnt,antiguedadPrestPrendarioActMasAnt,antiguedadTotalBSR,cantidadMesesInversionesTotal,cantidadMesesMBB,cantidadMesesSaldoCuenta,cuadranteCRM,montoCuotaTotalPrestHipotecario,montoCuotaTotalPrestPersonal,montoCuotaTotalPrestPrendario,montoMBBPromedio6M,montoMinimoInversionesTotal6M,montoMinimoMBBU6M,montoTenenciaAcciones,montoTenenciaBono,montoTenenciaPlazoFijo,promedioConsumosDebito6M,promedioConsumosTarjAmex6M,promedioConsumosTarjMaster6M,promedioConsumosTarjVisa6M,promedioExtraccionesDebito6M,promedioInversionesTotal6M,promedioSaldoCuenta6M,saldoDeudaAPTP,saldoDeudaPrestamoMono,tasa60MPPmono,consumo,limite,codTipoTarjeta,saldoNoUtilizado
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label like 'DatosFinancieros%';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmdatosfinancieros
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select omdm.tipo_tramite as cod_adm_tipotramite,
omdm.cod_tramite as cod_adm_tramite,
substr(omdm.fec_proceso,1,19) as ts_adm_fecproceso,
p.tipo_participante as cod_adm_tipoparticipante,
omdm.label as ds_adm_variable,
cast(omdm.antiguedadCuentaActMasAnt as int) as int_adm_antiguedadcuentaactmasant,
cast(omdm.antiguedadPrestHipotecarioActMasAnt as int) as int_adm_antiguedadpresthipotecarioactmasant,
cast(omdm.antiguedadPrestPersonalActMasAnt as int) as int_adm_antiguedadprestpersonalactmasant,
cast(omdm.antiguedadPrestPrendarioActMasAnt as int) as int_adm_antiguedadprestprendarioactmasant,
cast(omdm.antiguedadTotalBSR as int) as int_adm_antiguedadtotalbsr,
cast(omdm.cantidadMesesInversionesTotal as int) as int_adm_cantidadmesesinversionestotal,
cast(omdm.cantidadMesesMBB as int) as int_adm_cantidadmesesmbb,
cast(omdm.cantidadMesesSaldoCuenta as int) as int_adm_cantidadmesessaldocuenta,
omdm.cuadranteCRM as cod_adm_cuadrantecrm,
cast(omdm.montoCuotaTotalPrestHipotecario as double) as fc_adm_montocuotatotalpresthipotecario,
cast(omdm.montoCuotaTotalPrestPersonal as double) as fc_adm_montocuotatotalprestpersonal,
cast(omdm.montoCuotaTotalPrestPrendario as double) as fc_adm_montocuotatotalprestprendario,
cast(omdm.montoMBBPromedio6M as double) as fc_adm_montombbpromedio6m,
cast(omdm.montoMinimoInversionesTotal6M as double) as fc_adm_montominimoinversionestotal6m,
cast(omdm.montoMinimoMBBU6M as double) as fc_adm_montominimombbu6m,
cast(omdm.montoTenenciaAcciones as double) as fc_adm_montotenenciaacciones,
cast(omdm.montoTenenciaBono as double) as fc_adm_montotenenciabono,
cast(omdm.montoTenenciaPlazoFijo as double) as fc_adm_montotenenciaplazofijo,
cast(omdm.promedioConsumosDebito6M as double) as fc_adm_promedioconsumosdebito6m,
cast(omdm.promedioConsumosTarjAmex6M as double) as fc_adm_promedioconsumostarjamex6m,
cast(omdm.promedioConsumosTarjMaster6M as double) as fc_adm_promedioconsumostarjmaster6m,
cast(omdm.promedioConsumosTarjVisa6M as double) as fc_adm_promedioconsumostarjvisa6m,
cast(omdm.promedioExtraccionesDebito6M as double) as fc_adm_promedioextraccionesdebito6m,
cast(omdm.promedioInversionesTotal6M as double) as fc_adm_promedioinversionestotal6m,
cast(omdm.promedioSaldoCuenta6M as double) as fc_adm_promediosaldocuenta6m,
cast(omdm.saldoDeudaAPTP as double) as fc_adm_saldodeudaaptp,
cast(omdm.saldoDeudaPrestamoMono as double) as fc_adm_saldodeudaprestamomono,
cast(omdm.tasa60MPPmono as double) as fc_adm_tasa60mppmono,
cast(omdm.consumo as double) as fc_adm_consumo,
cast(omdm.limite as double) as fc_adm_limite,
omdm.codTipoTarjeta as cod_adm_codtipotarjeta,
cast(omdm.saldoNoUtilizado as double) as fc_adm_saldonoutilizado,
omdm.json as ds_adm_json
from datosfinancieros omdm
left join participantes p on p.tipo_tramite=omdm.tipo_tramite and p.cod_tramite=omdm.cod_tramite and p.participante=omdm.participante;
