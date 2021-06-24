
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_ben_clientespremify
partition(partition_date)

Select
c.nup as cod_per_nup,
c.last_name as ds_ben_apellido,
c.first_name as ds_ben_nombre,
c.id_level as cod_ben_nivel,
c.points as fc_ben_puntos,
c.docnumber as cod_ben_documento,
c.doctype as cod_ben_tipodocumento,
SUBSTRING (c.birthdate,1,10) as dt_ben_nacimientocliente,
case when c.id_audience  ='null' then null else c.id_audience  end  as cod_ben_audiencia,
c.accepted_tyc as flag_ben_aceptaterminos,
SUBSTRING (tc.datefrom,1,10) as dt_ben_desde,
tc.id_tag as cod_ben_tag,
t.name as ds_ben_tag,
t.description as ds_ben_tagdescripcion,
t.`group` as cod_ben_grupo,
t.exclusive as flag_ben_exlusivo,
t.slug as ds_ben_slug,
c.partition_date
from
bi_corp_staging.rio265_customer c

inner join
bi_corp_staging.rio265_tagcustomer tc
on
c.nup = tc.id_customer

left join bi_corp_staging.rio265_tag t
on
t.id = tc.id_tag
and t.partition_date = c.partition_date
Where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtcoo', dag_id='LOAD_CMN_Beneficios') }}'
and tc.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtcoo', dag_id='LOAD_CMN_Beneficios') }}'
and tc.id_tag = '-1000';
