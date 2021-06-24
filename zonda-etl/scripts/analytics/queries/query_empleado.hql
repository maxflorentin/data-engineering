SET mapred.job.queue.name=root.dataeng;
select cod_periodo, num_persona, 1 as Marca_Empleado_Santander
from santander_business_risk_arda.nomina_empleados
where cod_periodo >= '201901'