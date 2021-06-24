SELECT  feoperac,s1emp,contra1,ind_def,
        dias_pp,dias_atr,fec_def,fini_pp,
        ffin_pp,imp_atr,sonbldef,flgtecni,finalind
FROM 
(
	SELECT    'feoperac' AS feoperac,'s1emp' AS s1emp,'contra1' AS contra1,
              'ind_def'  AS ind_def,'dias_pp'  AS dias_pp,'dias_atr' AS dias_atr,
              'fec_def'  AS fec_def,'fini_pp'  AS fini_pp,'ffin_pp'  AS ffin_pp,
              'imp_atr'  AS imp_atr,'sonbldef' AS sonbldef, 'flgtecni' as flgtecni,
              'finalind' AS finalind 
) A;