"DROP TABLE IF EXISTS bi_corp_common.report_nps_ordenador_operativo;

CREATE TABLE bi_corp_common.report_nps_ordenador_operativo AS
select collect_list(json) from
(select named_struct("branch", cast(cod_nps_nro_ubicacion as int),
"npsBranch", abs(round((sum(case when ds_nps_agrupados = '3' then 1 else 0 end)-sum(case when ds_nps_agrupados = '1' then 1 else 0 end))/count(*),2)*100),
"npsCashAccount", abs(round((sum(case when ds_nps_agrupados = '3' and ds_nps_tipo_encuesta in ('CAJA','CAJA PYMES') then 1 else 0 end)-sum(case when ds_nps_agrupados = '1' and ds_nps_tipo_encuesta in ('CAJA','CAJA PYMES') then 1 else 0 end))/if(sum(case when ds_nps_tipo_encuesta in ('CAJA','CAJA PYMES') then 1 else 0 end) = 0, null, sum(case when ds_nps_tipo_encuesta in ('CAJA','CAJA PYMES') then 1 else 0 end)),2)*100),
"npsAtm", abs(round((sum(case when ds_nps_agrupados = '3' and ds_nps_tipo_encuesta = 'ATM' then 1 else 0 end)-sum(case when ds_nps_agrupados = '1' and ds_nps_tipo_encuesta = 'ATM' then 1 else 0 end))/if(sum(case when ds_nps_tipo_encuesta = 'ATM' then 1 else 0 end) = 0, null, sum(case when ds_nps_tipo_encuesta = 'ATM' then 1 else 0 end)),2)*100),
"npsTas", abs(round((sum(case when ds_nps_agrupados = '3' and ds_nps_tipo_encuesta = 'TAS' then 1 else 0 end)-sum(case when ds_nps_agrupados = '1' and ds_nps_tipo_encuesta = 'TAS' then 1 else 0 end))/if(sum(case when ds_nps_tipo_encuesta = 'TAS' then 1 else 0 end) = 0, null, sum(case when ds_nps_tipo_encuesta = 'TAS' then 1 else 0 end)),2)*100)) as json
from bi_corp_common.bt_nps_nps
where partition_date between '2021-01-01' and '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Branches-Orden-Operativo') }}'
and ds_nps_tipo_encuesta in ('PLATAFORMA','PLATAFORMA PYMES','ONBOARDING','ONBOARDING DIGITAL','SEGUROS','ATM','TAS','CAJA','CAJA PYMES')
and (ds_nps_canal in ('ATM','SUCURSAL') or ds_nps_tipo_encuesta = 'TAS')
and cast(cod_nps_nro_ubicacion as int) is not null
and ds_nps_orden = 1
group by cast(cod_nps_nro_ubicacion as int)
order by 1 asc limit 600) t;"