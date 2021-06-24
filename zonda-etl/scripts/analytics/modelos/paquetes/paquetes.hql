SET mapred.job.queue.name=root.dataeng;
select nup, score, binned as quantile from analytics.xsell_paquetes_propension_score_vigente;