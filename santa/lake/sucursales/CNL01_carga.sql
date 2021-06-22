CREATE OR REPLACE PACKAGE BODY CNL01."CNL01_CARGA" IS

FUNCTION VALIDA_TRANSAC_OP(p_canal_id IN VARCHAR2, p_tx_id IN VARCHAR2, p_ente_id IN VARCHAR2) RETURN BOOLEAN IS

v_tx_id VARCHAR2(6);

BEGIN
   SELECT distinct TX_ID INTO v_tx_id FROM cnl01.AGRUPAMIENTO WHERE cod_sector = 'BA' AND canal_id = p_canal_id AND tx_id = p_tx_id AND ente_id = p_ente_id;
   RETURN TRUE;

EXCEPTION
WHEN OTHERS THEN
    RETURN FALSE;
END VALIDA_TRANSAC_OP;


FUNCTION VALIDA_E_CARGA(p_canal_id IN VARCHAR2, p_tx_id IN VARCHAR2, p_ente_id IN VARCHAR2) RETURN BOOLEAN IS

v_canal_e VARCHAR2(5);

BEGIN
   SELECT CANAL_ID INTO v_canal_e FROM cnl01.ERROR_CARGA WHERE CANAL_ID = p_canal_id AND TX_ID    = p_tx_id AND ENTE_ID  = p_ente_id;
   RETURN TRUE;

EXCEPTION
WHEN OTHERS THEN
     RETURN FALSE;
END VALIDA_E_CARGA;


PROCEDURE TRUNC_ERROR IS

BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE CNL01.ERROR_CARGA';

EXCEPTION
  WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR AL TRUNCAR LAS TABLAS DE ERROR');
        DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
  RAISE;
END TRUNC_ERROR;


PROCEDURE Borro_Movimientos_rech(pi_fecha_proceso DATE) IS

BEGIN
     DELETE cnl01.MOVIMIENTO_RECH WHERE fecha_carga = pi_fecha_proceso;
END;


PROCEDURE Borro_Movimientos(pi_fecha_proceso DATE) IS
    vi_count NUMBER := 0;
BEGIN

    DBMS_OUTPUT.PUT_LINE ('Inicio BORRO_MOVIMIENTOS: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    SELECT COUNT(*) INTO vi_count
    FROM MOVIMIENTO
    WHERE FECHA >= pi_fecha_proceso - 5
    AND   FECHA <= pi_fecha_proceso + 3
    AND   FECHA_CARGA = pi_fecha_proceso
    AND ROWNUM = 1;

    IF vi_count = 1 THEN
        DELETE cnl01.MOVIMIENTO
        WHERE FECHA >= pi_fecha_proceso - 5
        AND   FECHA <= pi_fecha_proceso + 3
        AND   FECHA_CARGA = pi_fecha_proceso;
    END IF;

END;


PROCEDURE CARGA_MOVIMIENTOS_RECH (pi_fecha_proceso VARCHAR2)IS

   v_persona  VARCHAR2(3);
   v_error BOOLEAN;
   v_cant  NUMBER(10):=0;

CURSOR Cur_MovCarga  IS
    SELECT tipo_cliente, suc_nro, suc_maq, canal_id, maq_id, fecha, hora, tx_id, ente_id, orig_rev, fecha_proc, persona, nup, usuario, importe,
           moneda, cantidad_cheques, marca_man_aut, fecha_carga, ide_pago, cantidad_efectivo, cantidad_otros, importe_cheques, importe_efectivo,
           importe_otros, cuadrante, pesubseg, ROWID ROW_ID, medio_pago
      FROM cnl01.MOVIMIENTO_RECH;

vd_fecha_carga DATE;

BEGIN

     DBMS_OUTPUT.PUT_LINE ('Inicio CARGA_MOVIMIENTOS_RECH: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

     vd_fecha_carga := TO_DATE(pi_fecha_proceso,'YYYYMMDD');

     -- Actualizo las ternas
     cnl01.Cnl01_Carga.ACTUALIZAR_AGRUPAMIENTO;

     -- Llama a este proceso para borrar los errores anteriores
         cnl01.Cnl01_Carga.TRUNC_ERROR;

     --FDY 22/08/2003
     COMMIT;
 --    SET TRANSACTION USE ROLLBACK SEGMENT RBSBIG;   esta comentado porque no lo acepta en desarrollo

     FOR Cur_ppal IN Cur_MovCarga LOOP
       v_error := FALSE;
       BEGIN
         -- Validacisn contra la Tabla TRANSACCION_OPERACION
         BEGIN
            IF VALIDA_TRANSAC_OP(Cur_ppal.CANAL_ID, Cur_ppal.TX_ID, Cur_ppal.ENTE_ID) = FALSE
            THEN
                IF v_error = FALSE
                THEN
                    IF VALIDA_E_CARGA(Cur_ppal.CANAL_ID, Cur_ppal.TX_ID, Cur_ppal.ENTE_ID) = FALSE
                    THEN
                        BEGIN
                             INSERT INTO cnl01.ERROR_CARGA VALUES (Cur_ppal.CANAL_ID, Cur_ppal.TX_ID,Cur_ppal.ENTE_ID,  TO_DATE(Cur_ppal.FECHA_PROC,'yyyymmdd'),NULL);
                        EXCEPTION
                        WHEN OTHERS THEN
                          DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                          DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                          DBMS_OUTPUT.PUT_LINE('ERROR INSERT - ERROR_CARGA');
                          RAISE;
                        END;
                    END IF;
                    v_error := TRUE;
                END IF;
            END IF;
         EXCEPTION
         WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE( 'ERROR CARGA_MOVIMIENTOS - Validacisn TRANSACCION_OPERACION');
              RAISE;
         END;

  -- Chequea CODIGO TRANSACCION--080804--
         IF Cur_ppal.CANAL_ID= 'CAJA' AND Cur_ppal.TX_ID = '013359' AND Cur_ppal.CANTIDAD_CHEQUES > 0
         THEN
             Cur_ppal.TX_ID := '913359';
         END IF;
---  Susana 1
         IF v_error = FALSE
         THEN
             BEGIN
                 INSERT /*+ APPEND */
                 INTO CNL01.MOVIMIENTO (maq_id ,fecha ,hora ,tipo_cliente ,suc_nro ,suc_maq ,canal_id ,tx_id ,ente_id ,orig_rev ,fecha_proc ,nup ,usuario ,cuadrante ,importe ,moneda
                   ,cantidad_cheques ,pesubseg ,fecha_carga ,marca_man_aut ,ide_pago ,cantidad_efectivo ,cantidad_otros ,importe_cheques ,importe_efectivo,importe_otros ,medio_pago)
                 VALUES (cur_ppal.maq_id ,TO_DATE(cur_ppal.fecha,'yyyymmdd') ,cur_ppal.hora ,cur_ppal.tipo_cliente ,cur_ppal.suc_nro ,cur_ppal.suc_maq ,cur_ppal.canal_id ,cur_ppal.tx_id
                        ,cur_ppal.ente_id ,cur_ppal.orig_rev ,TO_DATE(cur_ppal.fecha_proc,'yyyymmdd') ,cur_ppal.nup ,cur_ppal.usuario ,cur_ppal.cuadrante ,cur_ppal.importe ,cur_ppal.moneda
                        ,cur_ppal.cantidad_cheques ,cur_ppal.pesubseg ,vd_fecha_carga ,cur_ppal.marca_man_aut ,cur_ppal.ide_pago ,cur_ppal.cantidad_efectivo ,cur_ppal.cantidad_otros
                        ,cur_ppal.importe_cheques ,cur_ppal.importe_efectivo ,cur_ppal.importe_otros ,cur_ppal.medio_pago);

                --DHC
                EXCEPTION
                WHEN OTHERS THEN
                   DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                   DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                   DBMS_OUTPUT.PUT_LINE('ERROR INSERT - MOVIMIENTO');
                   RAISE;
                END;

                --DHC
                BEGIN
                   DELETE FROM CNL01.MOVIMIENTO_RECH
                   WHERE ROWID = Cur_ppal.ROW_ID;
                EXCEPTION
                WHEN OTHERS THEN
                   DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                   DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                   DBMS_OUTPUT.PUT_LINE('ERROR DELETE - MOVIMIENTO_RECH');
                   RAISE;
                END;

         END IF;

       EXCEPTION
       WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
            DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
            DBMS_OUTPUT.PUT_LINE('SE HA INSERTADO UN REGISTRO EN LA TABLA DE MOVIMIENTOS RECHAZADOS');
            RAISE;
       END ;
       v_cant := v_cant + 1;
         --FDY 22/08/2003 IF ...
       IF v_cant >= 10000 THEN
           COMMIT;
           SET TRANSACTION USE ROLLBACK SEGMENT RBSBIG;
           v_cant := 0;
       END IF;
     END LOOP;

EXCEPTION
WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE('ERROR EN CARGA_MOVIMIENTOS_RECH' || CHR(10));
     DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
     DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
     RAISE;
END CARGA_MOVIMIENTOS_RECH;


PROCEDURE CARGA_MOVIMIENTOS(pi_fecha_proceso VARCHAR2)
IS

v_persona  VARCHAR2(3);
v_maq      VARCHAR2(6);
v_error BOOLEAN;

CURSOR Cur_MovCarga  IS
SELECT M.ROWID FILA,-- AGREGADO MS 03/09/2019
    m.tipo_cliente,
    m.suc_nro,
    m.suc_maq,
    m.canal_id,
    m.maq_id,
    --m.fecha,
    decode(ascii(substr(m.fecha,5,1)),0,m.fecha_proc,m.fecha) fecha,-- AGREGADO MS 03/09/2019
    m.hora,
    m.tx_id,
    m.ente_id,
    m.orig_rev,
    m.fecha_proc,
    m.persona,
    m.nup,
    m.usuario,
    m.importe,
    m.moneda,
    m.cantidad_cheques,
    m.marca_man_aut,
    a.cod_sector
    ,m.ide_pago
    ,m.cantidad_efectivo
    ,m.cantidad_otros
    ,m.importe_cheques
    ,m.importe_efectivo
    ,m.importe_otros
    ,m.cuadrante
    ,m.pesubseg
    ,m.medio_pago
FROM cnl01.MOVIMIENTO_CARGA m, cnl01.AGRUPAMIENTO a
WHERE a.cod_sector(+) = 'BA'
AND a.canal_id(+) = m.canal_id
AND a.tx_id(+) = m.tx_id
AND a.ente_id(+) = m.ente_id;

vd_fecha_carga DATE;

BEGIN

     DBMS_OUTPUT.PUT_LINE ('Inicio CARGA_MOVIMIENTOS: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

     vd_fecha_carga := TO_DATE(pi_fecha_proceso,'YYYYMMDD');

         -- Modificado Ms 03/09/2019
         DBMS_OUTPUT.PUT_LINE('vd_fecha_carga: '||vd_fecha_carga);
         
     -- Elimino registros para reproceso
     Borro_Movimientos(vd_fecha_carga);
     Borro_Movimientos_rech(vd_fecha_carga);

     FOR Cur_ppal IN Cur_MovCarga LOOP
           v_error := FALSE;
       BEGIN
          -- Chequea si el campo Persona viene o no Nulo
          IF Cur_ppal.PERSONA IS NULL OR Cur_ppal.PERSONA = '' THEN
             v_persona := Cur_ppal.TIPO_CLIENTE;
          ELSE
                 v_persona := Cur_ppal.PERSONA;
          END IF;
          -- Valido en CANAL_ID contra el TX_ID, ENTE_ID y MAQ_ID y si corresponde modifico el Maq_id = 'FM1'
          IF (Cur_ppal.canal_id = 'ADM' OR Cur_ppal.canal_id = 'CAJA') AND
                 (Cur_ppal.tx_id = '000770' OR Cur_ppal.tx_id = '000778' OR
                  Cur_ppal.tx_id = '008740' OR Cur_ppal.tx_id = '008748') AND
                 (Cur_ppal.ente_id ='000') AND (Cur_ppal.maq_id > '9999') THEN
                 v_maq := 'FM1';
          ELSE
             v_maq := Cur_ppal.maq_id;
          END IF;
          -- Validacisn contra la Tabla TRANSACCION_OPERACION
             BEGIN
                  IF Cur_ppal.COD_SECTOR IS NULL
                  THEN
                     IF v_error = FALSE
                     THEN
                         BEGIN
                            INSERT INTO
                            CNL01.MOVIMIENTO_RECH
                            (tipo_cliente ,suc_nro ,suc_maq ,canal_id ,maq_id ,fecha ,hora ,tx_id ,ente_id ,orig_rev ,fecha_proc ,persona ,error_logico,nup ,usuario ,importe ,moneda
                            ,cantidad_cheques ,marca_man_aut ,fecha_carga ,ide_pago ,cantidad_efectivo ,cantidad_otros ,importe_cheques ,importe_efectivo ,importe_otros ,cuadrante
                            ,pesubseg ,medio_pago)
                           VALUES
                            (v_persona, cur_ppal.suc_nro, cur_ppal.suc_maq, cur_ppal.canal_id, v_maq, cur_ppal.fecha, cur_ppal.hora, cur_ppal.tx_id, cur_ppal.ente_id, cur_ppal.orig_rev,
                             cur_ppal.fecha_proc, cur_ppal.persona, 'N', cur_ppal.nup, cur_ppal.usuario, cur_ppal.importe, cur_ppal.moneda, cur_ppal.cantidad_cheques, cur_ppal.marca_man_aut,
                             vd_fecha_carga,  cur_ppal.ide_pago ,cur_ppal.cantidad_efectivo,cur_ppal.cantidad_otros,cur_ppal.importe_cheques,cur_ppal.importe_efectivo,cur_ppal.importe_otros
                             ,cur_ppal.cuadrante,cur_ppal.pesubseg,cur_ppal.medio_pago);
                         EXCEPTION
                         WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                            DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                            DBMS_OUTPUT.PUT_LINE( 'ERROR INSERT MOVIMIENTO_RECH');
                            DBMS_OUTPUT.PUT_LINE('ROWID: '||Cur_ppal.FILA);-- AGREGADO MS 03/09/2019
                            RAISE;
                         END;

                         IF VALIDA_E_CARGA(Cur_ppal.CANAL_ID, Cur_ppal.TX_ID, Cur_ppal.ENTE_ID) = FALSE
                         THEN
                             BEGIN
                                 INSERT INTO cnl01.ERROR_CARGA VALUES (Cur_ppal.CANAL_ID, Cur_ppal.TX_ID, Cur_ppal.ENTE_ID,  TO_DATE(Cur_ppal.FECHA_PROC,'yyyymmdd'),NULL);
                            EXCEPTION
                            WHEN OTHERS THEN
                                 DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                                  DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                                  DBMS_OUTPUT.PUT_LINE( 'ERROR INSERT MOVIMIENTO_RECH');
                                  DBMS_OUTPUT.PUT_LINE('ROWID: '||Cur_ppal.FILA);-- AGREGADO MS 03/09/2019
                                  RAISE;
                            END;
                         END IF;
                         v_error := TRUE;

                     END IF;
                  END IF;

             EXCEPTION
             WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE( 'ERROR CARGA_MOVIMIENTOS_RECH - Validacisn TRANSACCION_OPERACION');
                    RAISE;
             END;

  -- Chequea CODIGO TRANSACCION--080804--
             IF Cur_ppal.CANAL_ID= 'CAJA' AND Cur_ppal.TX_ID = '013359' AND Cur_ppal.CANTIDAD_CHEQUES > 0 THEN
                Cur_ppal.TX_ID := '913359';
             END IF;
-- Susana 3
            IF v_error = FALSE THEN
               INSERT /*+ APPEND */
                 INTO CNL01.MOVIMIENTO
                 (maq_id ,fecha ,hora ,tipo_cliente ,suc_nro ,suc_maq ,canal_id ,tx_id ,ente_id ,orig_rev ,fecha_proc ,nup ,usuario,importe ,moneda ,cantidad_cheques
                 ,fecha_carga ,marca_man_aut ,ide_pago ,cantidad_efectivo ,cantidad_otros ,importe_cheques ,importe_efectivo ,importe_otros ,cuadrante ,pesubseg ,medio_pago)
              VALUES
                (v_maq ,TO_DATE(cur_ppal.fecha,'yyyymmdd') ,cur_ppal.hora ,v_persona ,cur_ppal.suc_nro ,cur_ppal.suc_maq ,cur_ppal.canal_id ,cur_ppal.tx_id ,cur_ppal.ente_id
                ,cur_ppal.orig_rev ,TO_DATE(cur_ppal.fecha_proc,'yyyymmdd') ,cur_ppal.nup ,cur_ppal.usuario ,cur_ppal.importe ,cur_ppal.moneda ,cur_ppal.cantidad_cheques ,vd_fecha_carga
                ,cur_ppal.marca_man_aut ,cur_ppal.ide_pago ,cur_ppal.cantidad_efectivo ,cur_ppal.cantidad_otros ,cur_ppal.importe_cheques ,cur_ppal.importe_efectivo
                ,cur_ppal.importe_otros ,cur_ppal.cuadrante ,cur_ppal.pesubseg ,cur_ppal.medio_pago);
            END IF;

          EXCEPTION
          WHEN OTHERS THEN
             BEGIN
               INSERT INTO
                CNL01.MOVIMIENTO_RECH
                 (tipo_cliente ,suc_nro ,suc_maq ,canal_id ,maq_id ,fecha ,hora ,tx_id ,ente_id ,orig_rev ,fecha_proc ,persona ,error_logico ,nup ,usuario ,importe ,moneda ,cantidad_cheques
                 ,marca_man_aut ,fecha_carga ,ide_pago ,cantidad_efectivo ,cantidad_otros ,importe_cheques ,importe_efectivo ,importe_otros ,cuadrante ,pesubseg ,medio_pago)
                VALUES
                 (cur_ppal.tipo_cliente, cur_ppal.suc_nro, cur_ppal.suc_maq, cur_ppal.canal_id, v_maq, cur_ppal.fecha, cur_ppal.hora, cur_ppal.tx_id, cur_ppal.ente_id, cur_ppal.orig_rev,
                  cur_ppal.fecha_proc, cur_ppal.persona, 'S', cur_ppal.nup, cur_ppal.usuario, cur_ppal.importe, cur_ppal.moneda, cur_ppal.cantidad_cheques, cur_ppal.marca_man_aut,
                  vd_fecha_carga,cur_ppal.ide_pago, cur_ppal.cantidad_efectivo, cur_ppal.cantidad_otros, cur_ppal.importe_cheques, cur_ppal.importe_efectivo, cur_ppal.importe_otros,
                  cur_ppal.cuadrante, cur_ppal.pesubseg, cur_ppal.medio_pago);

                  DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
                  DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
                  DBMS_OUTPUT.PUT_LINE('Se ha insertado un registro en la tabla de Movimientos Rechazados');
                  DBMS_OUTPUT.PUT_LINE('ROWID: '||Cur_ppal.FILA);-- AGREGADO MS 03/09/2019
                  
                  RAISE;
             END ;
       END ;
     END LOOP;

         EXCEPTION
      WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
         DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
         DBMS_OUTPUT.PUT_LINE( 'ERROR EN CARGA_MOVIMIENTOS');
         
     RAISE;

   END CARGA_MOVIMIENTOS;


   PROCEDURE ACTUALIZAR_AGRUPAMIENTO
   IS

  CURSOR Cur_Grupo  IS
  SELECT CANAL_ID, TX_ID,    ENTE_ID, COD_GRUPO
        FROM cnl01.ERROR_CARGA
        WHERE COD_GRUPO IS NOT NULL ;

   BEGIN

         FOR Cur_ppal IN Cur_Grupo LOOP
                 /*Actualiza la Tabla de Agrupamiento*/
                 BEGIN
                    INSERT INTO CNL01.AGRUPAMIENTO (COD_SECTOR, CANAL_ID, TX_ID, ENTE_ID, COD_GRUPO)
                    VALUES ('BA', Cur_ppal.CANAL_ID,Cur_ppal.TX_ID,Cur_ppal.ENTE_ID,  Cur_ppal.COD_GRUPO);
                 EXCEPTION
                 WHEN OTHERS THEN
                      DBMS_OUTPUT.PUT_LINE( 'ERROR ACTUALIZA_AGRUPAMIENTO - Insert AGRUPAMIENTO');
                      RAISE;
                 END;
         END LOOP;

         BEGIN
              DELETE cnl01.ERROR_CARGA WHERE COD_GRUPO IS NOT NULL;
               COMMIT;
         EXCEPTION
         WHEN OTHERS THEN
               DBMS_OUTPUT.PUT_LINE( 'ERROR - DELETE ERROR_CARGA');
              RAISE;
         END;

   EXCEPTION
   WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('CODIGO DE RETORNO: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE(' DESCRIPCION: ' || SUBSTR(SQLERRM, 1, 230));
        DBMS_OUTPUT.PUT_LINE( 'ERROR EN ACTUALIZAR_AGRUPAMIENTO');
        RAISE;

   END ACTUALIZAR_AGRUPAMIENTO;


PROCEDURE GRABA_LOG_ERRORES (pstr_error cnl01.LOG_ERRORES.description%TYPE) IS
BEGIN

            INSERT INTO cnl01.LOG_ERRORES (id_err, description) VALUES (s_log_errores_id.NEXTVAL, pstr_error);
                DBMS_OUTPUT.PUT_LINE (pstr_error);
                DBMS_OUTPUT.PUT_LINE (' ');

EXCEPTION
WHEN OTHERS THEN
     DBMS_OUTPUT.PUT_LINE( 'ERROR GRABA LOG ERRORES - INSERT LOG_ERRORES');
     RAISE;

END;


PROCEDURE Crea_Agrupa_Work (pi_fecha_proceso VARCHAR2) IS

BEGIN

    DBMS_OUTPUT.PUT_LINE( 'Crea_Agrupa_Work - COMIENZO ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    BEGIN

        EXECUTE IMMEDIATE 'DROP TABLE cnl01.agrupa_work';
        DBMS_OUTPUT.PUT_LINE( 'Se dropeo la tabla cnl01.agrupa_work - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    EXCEPTION
           WHEN OTHERS THEN
               IF SQLCODE = -942 THEN
                DBMS_OUTPUT.PUT_LINE( 'ERROR DROP "NO EXISTE" cnl01.agrupa_work: '||SUBSTR(SQLERRM( SQLCODE ),1,250) || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
            ELSE
                DBMS_OUTPUT.PUT_LINE( 'ERROR DROP cnl01.agrupa_work: ' ||SUBSTR(SQLERRM( SQLCODE ),1,250) || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
                   RAISE;
            END IF;
    END;

    --- se crea agrupa_work
    sql_stmt:= 'CREATE TABLE cnl01.agrupa_work '
                ||'TABLESPACE CNL01_DATA_M NOLOGGING AS '
                ||'SELECT DISTINCT '
                ||'m.canal_id, '
                ||'m.tx_id, '
                ||'''S'' informa_marca '
                ||'FROM cnl01.MOVIMIENTO m '
                ||'WHERE m.fecha_carga = '''|| TO_DATE(pi_fecha_proceso,'YYYYMMDD') || ''''
                ||'  AND m.marca_man_aut = ''M'' '
                ;

    BEGIN
        EXECUTE IMMEDIATE sql_stmt;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE( 'ERROR create cnl01.agrupa_work: ' ||SUBSTR(SQLERRM( SQLCODE ),1,250) || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
            RAISE;
    END;


    DBMS_OUTPUT.PUT_LINE( 'Se creo la tabla agrupa_work - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));


    --- se dan privilegios sobre agrupa_work
    BEGIN
        EXECUTE IMMEDIATE 'GRANT select, update, delete, insert ON cnl01.agrupa_work TO CNL01_CARGA1_ROL';
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE( 'ERROR al dar privilegios sobre cnl01.agrupa_work: ' ||SUBSTR(SQLERRM( SQLCODE ),1,250) || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
            RAISE;
    END;

    DBMS_OUTPUT.PUT_LINE('Se dieron los privilegios sobre la tabla cnl01.agrupa_work ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    DBMS_OUTPUT.PUT_LINE( 'Crea_Agrupa_Work - FINAL ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

END Crea_Agrupa_Work;


PROCEDURE Act_Agrup_Informa_Marca (pi_fecha_proceso VARCHAR2) IS
BEGIN

    DBMS_OUTPUT.PUT_LINE( 'Act_Agrup_Informa_Marca - COMIENZO ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    ---- se actualizan los campos informa_marca y fecha_informa de la tabla cnl01.agrupamiento
    sql_stmt:= 'UPDATE cnl01.AGRUPAMIENTO a '
               ||'SET a.informa_marca = ''S' 
               ||'a.fecha_informa = ''' ||TO_DATE(pi_fecha_proceso,'YYYYMMDD') || ''''
               ||'WHERE '
               ||'EXISTS (SELECT * '
               ||'          FROM cnl01.AGRUPA_WORK b '
               ||'         WHERE a.canal_id = b.canal_id '
               ||'           AND a.tx_id = b.tx_id) '
               ||'AND a.informa_marca IS NULL'
               ;

    BEGIN
        EXECUTE IMMEDIATE sql_stmt;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE( 'ERROR al actualizar cnl01.agrupamiento: ' ||SUBSTR(SQLERRM( SQLCODE ),1,250) || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
            RAISE;
    END;

    DBMS_OUTPUT.PUT_LINE( 'Act_Agrup_Informa_Marca - FINAL ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

END Act_Agrup_Informa_Marca;


/*actualiza cantidades e importes en movimiento SH 12/1/06 */
PROCEDURE Act_Importe_Cantidades (pi_fecha_proceso VARCHAR2) IS

  v_Cod_Retorno       VARCHAR2(2) := NULL ;
  vCantInsertadas     NUMBER(16):= 0;
  vCont           NUMBER(16):= 0;
  l_cantidad_efectivo NUMBER(16,2);
  l_importe_efectivo  NUMBER(16,2);
  l_cantidad_cheque   NUMBER(16,2);
  l_importe_cheque    NUMBER(16,2);
  l_cantidad_otro     NUMBER(16,2);
  l_importe_otro      NUMBER(16,2);
  l_fecha_proceso      cnl01.MOVIMIENTO.fecha%TYPE;



CURSOR cmov IS
SELECT a.ide_pgo
     , cantidad_efectivo
     , importe_efectivo
     , cantidad_cheque
     , importe_cheque
     , cantidad_otros
     , importe_otros
     , NVL(b.importe_efectivo,0)+ NVL(c.importe_cheque,0) + NVL(d.importe_otros,0) importe
FROM
     (SELECT ide_pgo
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso = l_fecha_proceso
      GROUP BY ide_pgo
      ) A
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_efectivo
             ,SUM(imp_mov) importe_efectivo
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago = 'E'
      GROUP BY ide_pgo
      ) B
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_cheque
             ,SUM(imp_mov) importe_cheque
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago = 'C'
      GROUP BY ide_pgo
      ) C
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_otros
             ,SUM(imp_mov) importe_otros
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago NOT IN ('E','C')
      GROUP BY ide_pgo
      ) D
WHERE a.ide_pgo       = b.ide_pgo(+)
  AND a.ide_pgo       = c.ide_pgo(+)
  AND a.ide_pgo       = d.ide_pgo(+)
;

CURSOR cFecha (pi_fecha IN VARCHAR2) IS
SELECT SUBSTR(ide_pago,1,8) fecha
FROM cnl01.MOVIMIENTO_CARGA
WHERE ide_pago IS NOT NULL
  AND TX_ID        IN  ('003940 003941')
  AND canal_ID      = 'CAJA'
  AND ROWNUM        = 1;


BEGIN

     DBMS_OUTPUT.PUT_LINE ('Inicio ACT_IMPORTE_CANTIDADES: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

  FOR rfec IN cFecha (pi_fecha_proceso)  LOOP
       l_fecha_proceso := TO_DATE(rfec.fecha,'yyyymmdd');
  END LOOP ;

  dbms_output.put_line('Fecha_proceso : ' || l_fecha_proceso);


  FOR reg IN cmov LOOP

   BEGIN

    UPDATE cnl01.MOVIMIENTO_CARGA
       SET Cantidad_efectivo =  NVL(reg.cantidad_efectivo,0)
          ,Importe_Efectivo  =  NVL(reg.importe_efectivo ,0)
            ,Cantidad_cheques  =  NVL(reg.cantidad_cheque  ,0)
          ,Importe_Cheques   =  NVL(reg.importe_cheque   ,0)
              ,Cantidad_otros    =  NVL(reg.cantidad_otros   ,0)
              ,Importe_otros     =  NVL(reg.importe_otros    ,0)
          ,Importe         =  NVL(reg.importe_otros    ,0) + NVL(reg.importe_efectivo ,0) + NVL(reg.importe_cheque   ,0)
    WHERE ide_pago               =  reg.ide_pgo;

    vCont := vCont + 1;

    EXCEPTION
     WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE( 'ERROR Actualizar cantidades_importes : ' || reg.ide_pgo || '  ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
            RAISE;
    END;

NULL;
  END LOOP;
-- proceso final

    dbms_output.put_line('Registros modificados en cnl01.movimientos : '||vCont);
    COMMIT;

END Act_Importe_Cantidades;


PROCEDURE actualizar_segm_cuadrante IS

vCant              NUMBER := 0;

CURSOR cmovcarga IS
SELECT a.ROWID,
       NVL(b.cuadrante,'I') cuadrante,
       NVL(SUBSTR(b.subsegmento,1,1),'I') pesubseg
FROM cnl01.MOVIMIENTO_CARGA a,
     cnl01.TEST_TCLIENTE_COPIA b
WHERE a.nup = b.nup(+)
  AND a.nup IS NOT NULL;

TYPE TyMovcarga IS TABLE OF cmovcarga%ROWTYPE
 INDEX BY PLS_INTEGER;

cur_array TyMovcarga;

BEGIN

    DBMS_OUTPUT.PUT_LINE ('Inicio ACTUALIZAR_SEGM_CUADRANTE: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));


  OPEN cmovcarga;

  LOOP
    FETCH cmovcarga BULK COLLECT INTO cur_array LIMIT 10000;

    FORALL i IN 1..cur_array.COUNT
    UPDATE cnl01.MOVIMIENTO_CARGA c
       SET cuadrante = cur_array(i).cuadrante,
           pesubseg  = cur_array(i).pesubseg
     WHERE c.ROWID = cur_array(i).ROWID;

    COMMIT;

    EXIT WHEN cmovcarga%NOTFOUND;

  END LOOP;

  CLOSE cmovcarga;

EXCEPTION
WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(SUBSTR('ERROR update movimiento_carga cuadrante-pesubseg : ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'),1,250));
    RAISE;

END actualizar_segm_cuadrante;


PROCEDURE actualizar_segm_cuadrante_men IS

vCant              NUMBER := 0;

CURSOR cmovim IS
    SELECT m.ROWID,
           c.subsegmento,
           c.cuadrante
    FROM cnl01.MOVIMIENTO m, cnl01.NUPS_CON_MOVIMIENTOS c
    WHERE m.fecha >= TO_DATE('01'|| TO_CHAR(ADD_MONTHS(SYSDATE,-1), 'MMYYYY'), 'DDMMYYYY')
    AND m.fecha <= LAST_DAY(TRUNC(ADD_MONTHS(SYSDATE,-1)))
    AND m.nup = c.nup
    AND m.nup IS NOT NULL
    AND m.pesubseg = 'I';

BEGIN

     DBMS_OUTPUT.PUT_LINE ('Inicio ACTUALIZAR_SEGM_CUADRANTE_MEN: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    EXECUTE IMMEDIATE 'TRUNCATE TABLE cnl01.nups_con_movimientos DROP STORAGE';

    ------------- carga nups_con_movimientos ------------
    BEGIN
        INSERT /*+ APPEND */ INTO cnl01.NUPS_CON_MOVIMIENTOS
        (SELECT DISTINCT nup, 'I I'
           FROM cnl01.MOVIMIENTO m
          WHERE m.fecha >= TO_DATE('01'|| TO_CHAR(ADD_MONTHS(SYSDATE,-1), 'MMYYYY'), 'DDMMYYYY')
            AND m.fecha <= LAST_DAY(TRUNC(ADD_MONTHS(SYSDATE,-1)))
            AND m.nup IS NOT NULL
            AND m.pesubseg = 'I'
        );
        COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SUBSTR('ERROR insert nups_con_movimientos : ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'),1,250));
        RAISE;
    END;

    ------------- actualiza nups_con_movimientos -------------
    BEGIN
        UPDATE cnl01.NUPS_CON_MOVIMIENTOS a
           SET (cuadrante, subsegmento) =  (SELECT cuadrante, SUBSTR(subsegmento,1,1)
                                              FROM cnl01.TEST_TCLIENTE_COPIA b
                                              WHERE a.nup = b.nup);
        COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SUBSTR('ERROR update nups_con_movimientos : ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'),1,250));
        RAISE;
    END;


    FOR reg IN cmovim LOOP

        BEGIN

            UPDATE cnl01.MOVIMIENTO c
            SET cuadrante = reg.cuadrante,
                pesubseg  = reg.subsegmento
            WHERE c.ROWID = reg.ROWID;

            vCant := vCant + 1;

            IF vCant = 1000 THEN
               vCant := 0;
               COMMIT;
            END IF;

        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(SUBSTR('ERROR update movimiento cuadrante-pesubseg : ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'),1,250));
            RAISE;
        END;

    END LOOP;

    COMMIT;

END actualizar_segm_cuadrante_men;


/*prc_act_medio_pago_importe*/
PROCEDURE prc_act_medio_pago_importe
IS

CURSOR CUR_mov_carga IS
SELECT DISTINCT TX_ID,
              MEDIO_PAGO
FROM MOVIMIENTO_CARGA;
BEGIN

    DBMS_OUTPUT.PUT_LINE ('Inicio PRC_ACT_MEDIO_PAGO_IMPORTE: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));


 dbms_output.put_line('CNL01_CARGA - COMIENZO - prc_act_medio_pago_importe' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

 FOR reg IN cur_mov_carga
  LOOP

   IF(((reg.tx_id = '003951') AND (reg.MEDIO_PAGO = '0'))
           OR
      (((reg.tx_id = '003934') OR (reg.tx_id= '013460')) AND (reg.MEDIO_PAGO = '01'))) THEN

            UPDATE MOVIMIENTO_CARGA
                SET MEDIO_PAGO  = 'E',
                importe_efectivo = importe,
                    importe_cheques = 0,
                    importe_otros = 0
            WHERE tx_id = reg.tx_id
            AND medio_pago = reg.medio_pago;

            COMMIT;
    ELSE
        IF ( ((reg.tx_id = '003951') AND (reg.MEDIO_PAGO = '1'))
               OR
             (((reg.tx_id = '003934') OR (reg.tx_id ='013460'))
               AND ( reg.MEDIO_PAGO = '02'
                      OR reg.MEDIO_PAGO = '03'
                   OR reg.MEDIO_PAGO = '04'
                  OR reg.MEDIO_PAGO = '05'
                  OR reg.MEDIO_PAGO = '06'
                  OR reg.MEDIO_PAGO = '07'
                  OR reg.MEDIO_PAGO = '08'
                  OR reg.MEDIO_PAGO = '09'
                  OR reg.MEDIO_PAGO = '10'
                  OR reg.MEDIO_PAGO = '12'))) THEN


            UPDATE MOVIMIENTO_CARGA
            SET MEDIO_PAGO  = 'C',
            importe_cheques = importe,
                importe_efectivo = 0,
            importe_otros = 0
            WHERE tx_id = reg.tx_id
            AND medio_pago = reg.medio_pago;

            COMMIT;

        ELSE
            IF (((reg.tx_id = '003951') AND (reg.MEDIO_PAGO = '2'))
                            OR
                (((reg.tx_id = '003934') OR (reg.tx_id = '013460')) AND (reg.MEDIO_PAGO = '00'))) THEN

                UPDATE MOVIMIENTO_CARGA
                SET MEDIO_PAGO  = 'D',
                importe_otros = importe,
                    importe_efectivo = 0,
                importe_cheques = 0
                WHERE tx_id = reg.tx_id
                AND medio_pago = reg.medio_pago;

                COMMIT;

            END IF;
        END IF;
     END IF;

   COMMIT;

 END LOOP;

 COMMIT;

 dbms_output.put_line('CNL01_CARGA - FIN - prc_act_medio_pago_importe' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SUBSTR('ERROR update movimiento_carga : ' || SQLERRM || ' - ' || TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'),1,250));
        RAISE;

END prc_act_medio_pago_importe;


PROCEDURE actualizar_movcarga (pi_fecha_proceso VARCHAR2) IS

vfecha date;

BEGIN

    DBMS_OUTPUT.PUT_LINE ('Inicio ACTUALIZAR_MOVCARGA: '|| TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));

    vfecha := to_date(pi_fecha_proceso,'YYYYMMDD');

    UPDATE cnl01.movimiento_carga
    SET fecha = TO_CHAR(vfecha - 1,'YYYYMMDD')
    WHERE fecha < '20010101';

    commit;

END actualizar_movcarga;

END;  /* GOLDENGATE_DDL_REPLICATION */
/
