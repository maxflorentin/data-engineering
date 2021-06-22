  PROCEDURE prc_calc_facturacion(p_fecha_proceso IN DATE)
  IS
  BEGIN
    --tabla auxiliar para el calculo de la facturacion
    EXECUTE IMMEDIATE ' TRUNCATE TABLE EST01.TEST_VTX_FACT_AUX ';

    --Tabla auxiliar que contiene ingresos de pedt036 y facturacion de pedt005
    EXECUTE IMMEDIATE ' TRUNCATE TABLE EST01.TEST_VTX_PEDT_AUX ';

    --Tabla auxiliara para almacenar la mediana por cuadrante
    EXECUTE IMMEDIATE ' TRUNCATE TABLE EST01.TEST_VTX_MEDI_AUX ';

    --inserta en tabla auxiliar para el calculo de la facturacion: TEST_VTX_FACT_AUX
    EXECUTE IMMEDIATE
         ' INSERT INTO EST01.TEST_VTX_FACT_AUX (NUP, NRO_SEGMENTO, CUADRANTE, FECHA_PROCESO,CREDITOS_90,FACT_ORIGINAL,FACTURACION) SELECT NUP, NRO_SEGMENTO, CUADRANTE, FECHA_PROCESO, 0, 0, 0 FROM EST01.TEST_CLIENTES_EPA WHERE FECHA_PROCESO = '''
      || p_fecha_proceso
      || '''';

    --INSERTO TABLA AUXILIAR CON LOS INGRESOS/FACTURCION DECLARADA
    INSERT INTO est01.test_vtx_pedt_aux(nup
                                       ,facturacion)
      SELECT fct.penumper
            ,fct.facturacion
      FROM (SELECT DISTINCT FIRST_VALUE(a.penumper) OVER (PARTITION BY penumper ORDER BY pefecing DESC) penumper
                           ,FIRST_VALUE(a.peimping * 12) OVER (PARTITION BY penumper ORDER BY pefecing DESC) facturacion
            FROM est01.pedt036 a
            WHERE a.pecdgent = '0072'
              AND a.petiping = '015'
              AND a.pefecing <= LAST_DAY(p_fecha_proceso)
            UNION
            SELECT penumper
                  ,facturacion
            FROM pedt005_aux
            WHERE fecha_proceso = p_fecha_proceso) fct
      WHERE EXISTS
              (SELECT 1
               FROM test_clientes_epa epa
               WHERE epa.fecha_proceso = p_fecha_proceso
                 AND epa.nup = fct.penumper);

    COMMIT;

    --Actualiza la columna fact_original, que contendrá siempre la facturacion segun los ingresos declarados en TEST_VTX_FACT_AUX
    UPDATE est01.test_vtx_fact_aux aux
    SET aux.fact_original      =
          (SELECT ft.facturacion
           FROM est01.test_vtx_pedt_aux ft
           WHERE ft.nup = aux.nup)
    WHERE EXISTS
            (SELECT 1
             FROM est01.test_vtx_pedt_aux ft
             WHERE ft.nup = aux.nup);

    COMMIT;

    --Actualiza tambien la columna facturacion que será la que contenga el valor final calculado de facturacion
    UPDATE est01.test_vtx_fact_aux aux
    SET aux.facturacion   = NVL(aux.fact_original, 0);

    COMMIT;

    --Actualizo el grupo al que pertence el nup
    UPDATE est01.test_vtx_fact_aux a
    SET grupo      =
          (SELECT g2.grupo
           FROM (SELECT penumper
                       ,MAX(grupo) grupo
                 FROM pedt025_aux g1
                 WHERE g1.fecha_proceso = p_fecha_proceso
                 GROUP BY penumper) g2
           WHERE g2.penumper = a.nup);

    COMMIT;

    --Actualizo la facturcion con la max(facturacion) del grupo obtenida de pedt036/pedt005
    UPDATE test_vtx_fact_aux a1
    SET facturacion      =
          (SELECT NVL(gr.facturac, 0)
           FROM (SELECT a2.grupo
                       ,MAX(a2.fact_original) facturac
                 FROM test_vtx_fact_aux a2
                 GROUP BY grupo) gr
           WHERE gr.grupo = a1.grupo)
    WHERE a1.grupo IS NOT NULL;

    COMMIT;

    --Actualiza el indicador si tiene creditos a 90 dias superior a un monto especifico por PLANILLA
    UPDATE est01.test_vtx_fact_aux aux
    SET aux.creditos_90      =
          (SELECT CASE
                    WHEN (b.planilla IN ('PYM1', 'COM'))
                     AND (a.max_saldo_cp_lp_le_90_p) > (creditos_p1_c1 * cotizparam)
                    THEN
                      1
                    WHEN (b.planilla = 'PYM2')
                     AND (a.max_saldo_cp_lp_le_90_p) > (creditos_p2 * cotizparam)
                    THEN
                      1
                    WHEN (b.planilla IN ('EMP', 'G1'))
                     AND (a.max_saldo_cp_lp_le_90_p) > (creditos_em_ge * cotizparam)
                    THEN
                      1
                    WHEN (b.planilla = 'MRG')
                     AND (a.max_saldo_cp_lp_le_90_p) > (creditos_cor * cotizparam)
                    THEN
                      1
                    WHEN (b.planilla = 'INS')
                     AND (a.max_saldo_cp_lp_le_90_p) > (creditos_ins * cotizparam)
                    THEN
                      1
                    ELSE
                      0
                  END
                    creditos_90
           FROM test_clientes a
               ,test_par_escalar_2_planillas b
           WHERE a.fecha_proceso = p_fecha_proceso
             AND a.ind_baja = 'N'
             AND a.nro_segmento = b.nro_segmento
             AND a.nup = aux.nup);

    COMMIT;

    -- Tabla auxiliar para contener la mediana por cuadrante
    INSERT INTO est01.test_vtx_medi_aux(cuadrante
                                       ,mediana_fact)
      SELECT cuadrante
            ,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facturacion DESC) mediana
      FROM est01.test_vtx_fact_aux
      WHERE NVL(facturacion, 0) > corte_fact --1200
        AND NVL(creditos_90, 0) > 0
      GROUP BY cuadrante;

    COMMIT;

    --Actualiza la facturcion con la mediana para los que tienen facturacion < 12000
    UPDATE test_vtx_fact_aux aux
    SET aux.facturacion      =
          (SELECT mediana_fact
           FROM test_vtx_medi_aux aux2
           WHERE aux.cuadrante = aux2.cuadrante)
    WHERE aux.facturacion < corte_fact --12000
      AND aux.cuadrante IS NOT NULL;

    COMMIT;

    --ACTUALIZA TEST_CLIENTES EPA con los datos obtenidos en TEST_VTX_FACT_AUX
    UPDATE est01.test_clientes_epa a
    SET (facturacion
        ,creditos_90
        ,grupo_max)      =
          (SELECT facturacion
                 ,creditos_90
                 ,grupo
           FROM est01.test_vtx_fact_aux b
           WHERE a.nup = b.nup)
    WHERE fecha_proceso = p_fecha_proceso;

    COMMIT;
  EXCEPTION
    WHEN OTHERS
    THEN
      DBMS_OUTPUT.put_line('ERROR PRC_CALC_FACTURACION');
      DBMS_OUTPUT.put_line(SUBSTR(SQLERRM(SQLCODE), 1, 250));
      RAISE;
  END prc_calc_facturacion;
  
  
Carga de las PEDT AUXILIARES:
============================

pedt005_aux

SELECT penumper, pefacanu, v_fecha_proceso
           FROM est01.pedt005
          WHERE pecdgent = '0072';
          	
pedt025_aux

SELECT penumpar, penumgru, v_fecha_proceso
           FROM est01.pedt025; 	 
           	
          