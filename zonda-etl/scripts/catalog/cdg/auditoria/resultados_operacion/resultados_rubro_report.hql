"set mapred.job.queue.name=root.dataeng;
select fec_data, cod_agrp_func, cod_centro_cont,
sum(cast(imp_ing_cap_ml_acum as float)),
sum(cast(imp_egr_cap_ml_acum as float)),
sum(cast(imp_ing_cap_ml as float)),
sum(cast(imp_egr_cap_ml as float)),
sum(cast(imp_ing_cap_ml_acum as double)),
sum(cast(imp_egr_cap_ml_acum as double)),
sum(cast(imp_ing_cap_ml as double)),
sum(cast(imp_egr_cap_ml as double)),
sum(cast(imp_ing_cap_ml_acum as bigint)),
sum(cast(imp_egr_cap_ml_acum as bigint)),
sum(cast(imp_ing_cap_ml as bigint)),
sum(cast(imp_egr_cap_ml as bigint))
from bi_corp_staging.rio157_ms0_ft_dwh_blce_result
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='REPORT_CDG_AUDITORIA_RESULTADOS_OPERACION') }}'
and cod_agrp_func in ('541003000036',
'541003000110',
'541003000031',
'541003000090',
'541003000091',
'541003000098',
'541003000153',
'5411345',
'541003000096',
'541003000094',
'541003000071',
'541003000092',
'5451504',
'5451002',
'5410059',
'511047000005',
'511047000077',
'5123131',
'511047000011',
'511047000078',
'5123204',
'511047000060',
'515070000064',
'5164202',
'515070000001',
'511048000256',
'511048000259',
'511048000257',
'511048000300',
'511048000301',
'511048000258',
'511048000042',
'511048000227',
'5123180',
'511048000068',
'511048000277',
'511048000598',
'511048000168',
'511048000067',
'511048000080',
'5168002',
'515048000106',
'515048000005',
'511048000272',
'511048000261',
'511048000270',
'511048000400',
'511047000201',
'511047000267',
'511052000267',
'511052000265',
'511052000153',
'511052000244',
'511052000240',
'511052000248',
'511052000001',
'511052000002',
'511052003001',
'5122332',
'5123271',
'5122471',
'5122333',
'5123347',
'511054000002',
'5123487',
'5123211',
'5510254',
'511053000107',
'511053000106',
'511053000088',
'511053000025',
'511053000041',
'511053000087',
'511053000072',
'511053000118',
'511053000038',
'511053000026',
'511053000092',
'511053000094',
'511053000208',
'511053000206',
'511053000111',
'511053000110',
'511053000069',
'511049000593',
'511049000100',
'511049000597',
'515048000046',
'515070000067',
'515070000068',
'515070000073',
'515070000069',
'5115018',
'511004000001',
'511004000603')
group by fec_data, cod_agrp_func, cod_centro_cont
"