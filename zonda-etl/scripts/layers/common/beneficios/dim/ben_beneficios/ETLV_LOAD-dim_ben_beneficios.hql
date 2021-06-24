set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_ben_beneficios
partition(partition_date)

SELECT DISTINCT
b.id                as cod_ben_beneficio,
b.name              as ds_ben_beneficio,
b.description       as ds_ben_descripcion,
SUBSTRING (b.datefrom ,1,10)         as dt_ben_desde,
SUBSTRING (b.dateto ,1,10)          as dt_ben_hasta,
case when b.authorizationuser ='null' then null else b.authorizationuser end as ds_ben_usuarioautorizacion,
case when b.modificationuser  ='null' then null else b.modificationuser end as ds_ben_usuariomodificacion,
case when b.hasautomaticrenewal  ='null' then null else b.hasautomaticrenewal end as cod_ben_renovacionaut,
case when b.hasretroactiveedition  ='null' then null else b.hasretroactiveedition end as cod_ben_acreditacionretro,
SUBSTRING (b.originaldatefrom ,1,10)  as dt_ben_desdeoriginal,
SUBSTRING (b.originaldateto ,1,10)   as dt_ben_hastaoriginal,
case when b.investment   ='null' then null else b.investment end  as cod_ben_inversion,
case when b.id_award    ='null' then null else b.id_award end  as cod_ben_premio,
case when b.id_benefitstate    ='null' then null else b.id_benefitstate end  as cod_ben_estado,
case
when SUBSTRING (b.pausedate ,1,10) ='null' then null
when SUBSTRING (b.pausedate ,1,4) = '2999' then null else SUBSTRING (b.pausedate ,1,10)  end   as dt_ben_pausa,
case when b.termsandconditions ='null' then null else b.termsandconditions  end   as ds_ben_terminosycondiciones,
case when t3.id_benefit     ='null' then null else t3.id_benefit  end   as cod_ben_categoriabeneficio,
case when t3.id_operationalcode    ='null' then null else t3.id_operationalcode  end   as cod_ben_operacion,
case when br.observations    ='null' then null else br.observations   end  as ds_ben_observaciones,
case when br.hasconsumptiondetail    ='null' then null else br.hasconsumptiondetail   end  as ds_ben_detalle,
nb.id_level         		as cod_ben_nivel,
nb.accreditationpercentage 	as fc_ben_porcentajeacreditacion,
nb.topamount        		as fc_ben_montotope,
case when nb.fixedamount   ='null' then null else nb.fixedamount  end  as fc_ben_montofijo,
case when nb.conditionamount  ='null' then null else nb.conditionamount end  as fc_ben_condicionmonto,
case when nb.toptransactions   ='null' then null else nb.toptransactions  end  as fc_ben_transacciontope,
case when brp.id_benefitrefund   ='null' then null else brp.id_benefitrefund   end  as cod_ben_reembolso,
case when brp.id_paymentmethod  ='null' then null else brp.id_paymentmethod  end  as cod_ben_metodopago,
case when br.id_operationalcode ='null' then null else br.id_operationalcode end  as cod_ben_reembolsocodigooperacion,
case when mp.name  ='null' then null else mp.name  end  as ds_ben_metodopago,
case when mp.codprisma  ='null' then null else mp.codprisma   end  as cod_ben_prisma,
case when mp.id_paymentmethodtype ='null' then null else mp.id_paymentmethodtype   end  as cod_ben_tipometodopago,
b.partition_date as partition_date

FROM

bi_corp_staging.rio265_benefit b
left join
bi_corp_staging.rio265_benefitrefund br
on br.id_benefit = b.id
and br.partition_date = b.partition_date
left join
bi_corp_staging.rio265_benefitrefpaymentmethod brp
on brp.id_paymentmethod = br.id_benefit
and brp.partition_date = br.partition_date
left join
bi_corp_staging.rio265_paymentmethod mp
on mp.id = brp.id_paymentmethod
and mp.partition_date = brp.partition_date

left join bi_corp_staging.rio265_benefitcategory t3
on t3.id_benefit = br.id_benefit
and t3.partition_date = br.partition_date
left join
bi_corp_staging.rio265_benefitlevel nb
on br.id_benefit = nb.id_benefit
and br.partition_date = nb.partition_date

Where b.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtcoo', dag_id='LOAD_CMN_Beneficios') }}'
and b.id_benefitstate in (2, 7, 9);
