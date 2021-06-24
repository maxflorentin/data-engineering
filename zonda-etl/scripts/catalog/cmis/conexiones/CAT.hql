"SET mapred.job.queue.name=root.dataeng;
 SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D6.AR.1.2.1' c,'Total CET1' d,'D6' e,'1' f,'D6.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D6.AR.1.2.2' c,'Aditional CET1' d,'D6' e,'1' f,'D6.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D6.AR.1.2' c,'Total TIER 1' d,'D6' e,'0' f,'D6.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D6.AR.1.1' c,'PNC' d,'D6' e,'1' f,'D6.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D6.AR.1' c,'Own Funds' d,'D6' e,'0' f,'D6.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D13.AR.1' c,'Ratio Core Capital ( CET1 / RWA Total)' d,'D13' e,'1' f,'D13.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D13.AR.2' c,'Ratio Tier 1 (Tier1 / RWA Total)' d,'D13' e,'1' f,'D13.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D13.AR.3' c,'Ratio Total ( Tier 2 / RWA Total)' d,'D13' e,'1' f,'D13.AR.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D13.AR.4' c,'Leverage ratio' d,'D13' e,'1' f,'D13.AR.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D14.AR.1' c,'Total RWA' d,'D14' e,'0' f,'D14.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D14.AR.1.1' c,'Credit Risk' d,'D14' e,'1' f,'D14.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D14.AR.1.2' c,'Market Risk' d,'D14' e,'1' f,'D14.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D14.AR.1.3' c,'Operational Risk' d,'D14' e,'1' f,'D14.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1' c,'ACTIVO' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.1' c,'Disponibilidades' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.1.1' c,'En pesos en el país' d,'D10' e,'1' f,'D10.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.1.2' c,'En pesos -En el exterior' d,'D10' e,'1' f,'D10.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.1.3' c,'En oro y moneda extranjera -En el país' d,'D10' e,'1' f,'D10.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.1.4' c,'En oro y moneda extranjera -En el exterior' d,'D10' e,'1' f,'D10.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.2' c,'Títulos Públicos y Privados' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.2.1' c,'En pesos' d,'D10' e,'1' f,'D10.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.2.2' c,'En moneda extranjera -Del país' d,'D10' e,'1' f,'D10.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.2.3' c,'En moneda extranjera -Del exterior' d,'D10' e,'1' f,'D10.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3' c,'Préstamos' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.1' c,'En pesos - Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.5' c,'Ajuste NIIF en pesos' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.3.6' c,'Ajuste NIIF en moneda extranjera' d,'D10' e,'1' f,'D10.AR.1.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.4' c,'O.C.I.F.' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.4.1' c,'En pesos -Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.4.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.4.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.4.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.5' c,'Créditos por arrendamientos financieros' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.5.1' c,'En pesos' d,'D10' e,'1' f,'D10.AR.1.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.5.2' c,'En moneda extranjera' d,'D10' e,'1' f,'D10.AR.1.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.6' c,'Participaciones en otras sociedades' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.6.1' c,'En pesos' d,'D10' e,'1' f,'D10.AR.1.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.6.2' c,'En moneda extranjera' d,'D10' e,'1' f,'D10.AR.1.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.7' c,'Créditos diversos' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.7.1' c,'En pesos -Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.7' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.7.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.7' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.7.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.1.7' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.7.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.1.7' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.8' c,'Bienes de uso' d,'D10' e,'1' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.9' c,'Bienes diversos' d,'D10' e,'1' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.10' c,'Bienes intangibles' d,'D10' e,'1' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.11' c,'Filiales en el exterior' d,'D10' e,'1' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.12' c,'Partidas pendientes de imputación - Saldos deudores' d,'D10' e,'0' f,'D10.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.12.1' c,'En pesos' d,'D10' e,'1' f,'D10.AR.1.12' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.1.12.2' c,'En moneda extranjera' d,'D10' e,'1' f,'D10.AR.1.12' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2' c,'PASIVO' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.1' c,'Depósitos' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.1.1' c,'En pesos -Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.1.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.1.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.1.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.2' c,'O.O.I.F' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.2.1' c,'En pesos - Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.2.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.2.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.2.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.3' c,'Obligaciones diversas' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.3.1' c,'En pesos -Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.3.2' c,'En pesos -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.3.3' c,'En moneda extranjera -Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.3.4' c,'En moneda extranjera -Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.3' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.4' c,'Previsiones' d,'D10' e,'1' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.5' c,'Partidas pendientes de imputación - Saldos acreedores' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.5.1' c,'En pesos' d,'D10' e,'1' f,'D10.AR.2.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.5.2' c,'En moneda extranjera' d,'D10' e,'1' f,'D10.AR.2.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.6' c,'Obligaciones subordinadas' d,'D10' e,'0' f,'D10.AR.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.6.1' c,'En pesos - Residentes en el país' d,'D10' e,'1' f,'D10.AR.2.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.6.2' c,'En pesos - Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.6.3' c,'En moneda extranjera - Residentes en el pa¡s' d,'D10' e,'1' f,'D10.AR.2.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D10.AR.2.6.4' c,'En moneda extranjera - Residentes en el exterior' d,'D10' e,'1' f,'D10.AR.2.6' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.1' c,'Ingresos financieros' d,'D11' e,'0' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.1.1' c,'Por Operaciones en Pesos' d,'D11' e,'1' f,'D11.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.1.2' c,'Por Operaciones en Moneda Extranjera' d,'D11' e,'1' f,'D11.AR.1.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.2' c,'Egresos financieros' d,'D11' e,'0' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.2.1' c,'Por Operaciones en Pesos' d,'D11' e,'1' f,'D11.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.2.2' c,'Por Operaciones en Moneda Extranjera' d,'D11' e,'1' f,'D11.AR.1.2' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.3' c,'Cargo por incobrabilidad' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.4' c,'Ingresos por servicios' d,'D11' e,'0' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.4.1' c,'Por Operaciones en Pesos' d,'D11' e,'1' f,'D11.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.4.2' c,'Por Operaciones en Moneda Extranjera' d,'D11' e,'1' f,'D11.AR.1.4' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.5' c,'Egresos por servicios' d,'D11' e,'0' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.5.1' c,'Por Operaciones en Pesos' d,'D11' e,'1' f,'D11.AR.1.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.5.2' c,'Por Operaciones en Moneda Extranjera' d,'D11' e,'1' f,'D11.AR.1.5' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.6' c,'Gastos de administración' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.7' c,'Utilidades diversas' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.8' c,'Pérdidas diversas' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.9' c,'Resultado de filiales en el exterior' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.10' c,'Impuesto a las ganancias' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.11' c,'Resultados monetarios' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1.12' c,'Otros Resultados Integrales (ORI)' d,'D11' e,'1' f,'D11.AR.1' g
 UNION ALL SELECT 'AR' a,'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' b,'D11.AR.1' c,'Total Resultados Acumulados' d,'D11' e,'0' f,'D11.AR.1' g "