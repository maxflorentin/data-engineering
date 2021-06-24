SET mapred.job.queue.name=root.dataeng;
SELECT 
       extract(year from FECHA_PROCESO)*100 + extract(month from FECHA_PROCESO) aniomes,
       extract(year from FECHA_PROCESO)*12 + extract(month from FECHA_PROCESO) nromes,
       A.NUP,
       MON_INGRESO,
       CANT_MOV_CC_CA_TOT,
       CONSUMO_TC,
       CONSUMO_TD,
       CANT_COMPRAS_TD,
       CANT_CONS_1ERCUOTA_FACT_MES,
       CANT_PREST_HIP_P + CANT_PREST_HIP_D as CANT_PREST_HIP,
       CANT_PREST_CONSUMO_OTROS_P + CANT_PREST_CONSUMO_OTROS_D as CANT_PREST_CONS,
       CANT_PREST_CONSUMO_AUTOS_P + CANT_PREST_CONSUMO_AUTOS_D as CANT_PREST_AUTO,
       CANT_SEGUROS,
       CANT_PAGA_DEBITO_AUTOMATICO,
       CANT_PAGA_DEBITO_AUT3MES,
       MBB_PROMEDIO_NMES,
       IND_COBRA_PLAN_SUELDO,
       IND_CUENTAS,
       CUADRANTE,
       CLIENTE_ACTIVO_SEG,
       IND_TIENE_CAJA_SEGURIDAD,
       CLIENTE_DE_MAS_VALOR,
       CANT_PAGOS_CUENTA_90,
       SALDO_SEGUROS_OM_P,
       floor(months_between(LAST_DAY(a.fecha_proceso),a.fecha_nacimiento) /12)  AS EDAD,
       (LAST_DAY(a.fecha_proceso)- a.fecha_ult_mov)                             AS Q_DIAS_ULTIMA_TRANSACCION,
       floor(months_between(LAST_DAY(a.fecha_proceso),a.fecha_alta))            AS Q_MESES_ALTA
FROM EST01.TEST_CLIENTES A 
WHERE  1=1 AND A.IND_BAJA = 'N' AND TIPO_PERSONA = 'F' AND MARCA_BANCA_PRIVADA = 'N' AND 
       FECHA_PROCESO between date'{}' and date'{}'