SELECT DISTINCT CONCAT(G4134_FEOPERAC, 
                       G4134_S1EMP, 
                       G4134_BIENGAR1, 
                       G4134_FECHVALR, 
                       G4134_NOMENTI, 
                       G4134_IMGARANT, 
                       G4134_FECULTMO, 
                       G4134_TIPVALN, 
                       G4134_TIP_GARA, 
                       G4134_CODDIV) AS line 
FROM bi_corp_bdr.JM_VAL_GARA 
WHERE partition_date = '$month_partition_bdr';