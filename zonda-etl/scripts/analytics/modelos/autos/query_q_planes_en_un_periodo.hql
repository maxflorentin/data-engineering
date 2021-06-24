SET mapred.job.queue.name=root.dataeng;
SELECT  X.COD_RAMO,
        X.COD_PRODUCTO,
        X.COD_PLAN,
        COUNT(*) AS Q_PLANES
FROM

(SELECT DATA_DATE_PART,
        COD_RAMO, 
        COD_PRODUCTO,
        COD_PLAN
       
FROM santander_business_risk_arda.seguros_operaciones

WHERE   DATA_DATE_PART BETWEEN '{fecha_ini}' AND '{fecha_fin}'
        AND COD_OPERACION IN (1001,1004) 
        AND COD_RAMO in ('30','31','32','33','34','35','36','37','38','39')
        AND COD_TIP_PRODUCTO = 'OM') X
        
GROUP BY X.COD_RAMO, X.COD_PRODUCTO, X.COD_PLAN
ORDER BY Q_PLANES DESC
