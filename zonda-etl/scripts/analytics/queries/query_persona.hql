'SET mapred.job.queue.name=root.dataeng;
select num_persona,cod_estado_civil_cliente,sexo_cliente,cod_tip_ocupacion_cliente,cod_actividad
from santander_business_risk_arda.personas
where data_date_part = '{}' and num_persona is not null'