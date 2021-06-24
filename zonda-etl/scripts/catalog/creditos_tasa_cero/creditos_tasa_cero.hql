SET mapred.job.queue.name=root.dataeng;
select concat_ws(';',cuenta,fpres,cast(importe1 as string),cast(importe2 as string),cast(importe3 as string),cast(importe4 as string),cast(importe_origen as string))
from
(SELECT coalesce(cast(600_usuario as string),'') cuenta,
coalesce(cast(600_fpres as string),'') fpres,
sum(600_importe_1) importe1,
sum(600_importe_2) importe2,
sum(600_importe_3) importe3,
sum(600_importe_4) importe4,
sum(600_imporig) importe_origen
FROM bi_corp_staging.aftc_waftc600 w600
WHERE 600_id_archivo = 'P'
AND 600_cod_registro = '30'
and partition_date='$partition_date'
and 600_codop = 199003
group by 600_usuario,600_fpres,600_codop)t;