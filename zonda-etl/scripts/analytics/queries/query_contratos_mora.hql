"SET mapred.job.queue.name=root.dataeng;
SELECT num_persona,
        max(ind_demora_1_30) as FLAG_MORA_30D, 
        max(ind_demora_31_60) as FLAG_MORA_60D, 
        max(ind_demora_61_90) as FLAG_MORA_90D, 
        max(case when ind_demora_91_120 + ind_demora_121_150 + ind_demora_151_180 + ind_demora_mas_180 >0 then 1 else 0 end) as FLAG_MORA_MAS_90
FROM santander_business_risk_arda.contratos
WHERE data_date_part in {}
AND num_persona is not null
GROUP BY num_persona"