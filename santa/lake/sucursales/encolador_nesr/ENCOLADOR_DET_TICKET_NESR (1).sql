PROCEDURE EXPORTAR_DATOS_TICKET_CLIENTE (direccion           VARCHAR2,
                                            nombreArchivo       VARCHAR2,
                                            fecha               VARCHAR2,
                                            extension           VARCHAR2,
                                            resultado       OUT NUMBER)
   IS
      file   UTL_FILE.FILE_TYPE;
   BEGIN
      resultado := 0;
      contador := 0;
      longitudAux := '                                                  '; --50 caracteres de long auxiliar.

      IF fecha IS NULL
      THEN
         SELECT SYSDATE INTO fechaHoy FROM DUAL;
      ELSE
         fechaHoy := TO_DATE (fecha, 'YYYYMMDD');
      END IF;

      IF nombreArchivo IS NULL
      THEN
         nombreArchivoAux :=
            'ENCOLADOR_DET_TICKET_NESR_' || TO_CHAR (fechaHoy, 'YYYYMMDD');
      ELSE
         nombreArchivoAux := nombreArchivo;
      END IF;

      file :=
         UTL_FILE.FOPEN (direccion,
                         nombreArchivoAux || '.' || extension,
                         'W');

      FOR registro
         IN (SELECT TICKETAUD.FECHA_CREACION fecha,
                    NVL2 (oficina.OFICINA_DEPENDE_ID,
                          (SELECT numero
                             FROM NESR_ONLINE.oficina
                            WHERE oficina_id = oficina.OFICINA_DEPENDE_ID),
                          oficina.NUMERO)
                       sucursal,
                    oficina.ZONA zona,
                    CASE
                       WHEN (SELECT UPPER (t4.ESTADO)
                               FROM NESR_ONLINE.TICKET_AUD t4
                              WHERE     t4.TICKET_ID = TICKETAUD.TICKET_ID --Se agrega el filtro por TICKET_ID por duplicitad del REV
                                    AND t4.REV =
                                           (SELECT MAX (t4_1.REV)
                                              FROM NESR_ONLINE.TICKET_AUD t4_1
                                             WHERE     t4_1.TICKET_ID =
                                                          TICKETAUD.TICKET_ID
                                                   AND t4_1.REV <
                                                          TICKETAUD.REV
                                                   AND t4_1.ESTADO IN
                                                          ('ESPERANDO',
                                                           'FLOTANTE'))) =
                               'FLOTANTE'
                       THEN
                          'Flotante'
                       WHEN UPPER (cola.NOMBRE) = 'CITADOS'
                       THEN
                          'Citado'
                       WHEN UPPER (cola.NOMBRE) = 'DERIVADOS'
                       THEN
                          'Derivado'
                       ELSE
                          cola.NOMBRE
                    END
                       tipo_cola,
                       LPAD (TICKETAUD.LETRA, 2, ' ')
                    || LPAD (TICKETAUD.NUMERO, 3, 0)
                       ticket,
                    servicio.NUMERO numero_servicio,
                    servicio.NOMBRE nombre_servicio,
                    puesto.NUMERO numero_puesto,
                    puesto.DESCRIPCION nombre_puesto,
                       TICKETAUD.LEGAJO_OPERADOR
                    || ' '
                    || TRIM (TICKETAUD.NOMBRE_OPERADOR)
                    || ', '
                    || TRIM (TICKETAUD.APELLIDO_OPERADOR)
                       usuario,
                    colaServicio.TIEMPOOBJESP t_obj_espera,
                    colaServicio.TIEMPOMAXESP t_max_espera,
                    (SELECT t2.FECHA_MODIFICACION
                       FROM NESR_ONLINE.TICKET_AUD t2
                      WHERE     t2.TICKET_ID = TICKETAUD.TICKET_ID --Se agrega el filtro por TICKET_ID por duplicitad del REV
                            AND t2.REV =
                                   (SELECT MAX (t2_1.REV)
                                      FROM NESR_ONLINE.TICKET_AUD t2_1
                                     WHERE     t2_1.TICKET_ID =
                                                  TICKETAUD.TICKET_ID
                                           AND t2_1.REV < TICKETAUD.REV
                                           AND t2_1.ESTADO IN
                                                  ('ESPERANDO', 'FLOTANTE')))
                       fecha_ingreso,
                    TICKETAUD.FECHA_MODIFICACION fecha_llamado,
                    (SELECT t3.FECHA_MODIFICACION
                       FROM NESR_ONLINE.TICKET_AUD t3
                      WHERE     t3.TICKET_ID = TICKETAUD.TICKET_ID --Se agrega el filtro por TICKET_ID por duplicitad del REV
                            AND t3.REV =
                                   (SELECT MIN (t3_1.REV)
                                      FROM NESR_ONLINE.TICKET_AUD t3_1
                                     WHERE     t3_1.TICKET_ID =
                                                  TICKETAUD.TICKET_ID
                                           AND t3_1.REV >= TICKETAUD.REV
                                           AND t3_1.ESTADO IN
                                                  ('ESPERANDO',
                                                   'FLOTANTE',
                                                   'FINALIZADO',
                                                   'ABANDONADO')))
                       fecha_fin_atencion,
                    oficina.TIEMPO_DELAY_EMISION delayEmision,
                    oficina.TIEMPO_DERIVACION delayDerivacion,
                    MOTIVO_VISITA.MOTIVO_VISITA_ID cod_motivo,
                    MOTIVO_VISITA.NOMBRE nom_motivo,
                    (SELECT TAUD.ESTADO
                       FROM NESR_ONLINE.TICKET_AUD TAUD
                      WHERE     TICKETAUD.TICKET_ID = TAUD.TICKET_ID
                            AND TAUD.REV =
                                   (SELECT MAX (TAUD_1.REV)
                                      FROM NESR_ONLINE.TICKET_AUD TAUD_1
                                     WHERE TAUD_1.TICKET_ID =
                                              TICKETAUD.TICKET_ID))
                       ESTADO,
                    (SELECT MA.NOMBRE
                       FROM NESR_ONLINE.MOTIVO_ABANDONO MA
                      WHERE MA.MOTIVO_ABANDONO_ID =
                               (SELECT TAUD_2.MOTIVO_ABANDONO_ID
                                  FROM NESR_ONLINE.TICKET_AUD TAUD_2
                                 WHERE     TICKETAUD.TICKET_ID =
                                              TAUD_2.TICKET_ID
                                       AND TAUD_2.REV =
                                              (SELECT MAX (TAUD_2.REV)
                                                 FROM NESR_ONLINE.TICKET_AUD TAUD_2
                                                WHERE TAUD_2.TICKEt_ID =
                                                         TICKETAUD.TICKET_ID)))
                       MOTIVO_ABANDONO,
                    (TICKETAUD.TECN * 100) TECN
               FROM NESR_ONLINE.TICKET_AUD TICKETAUD
                    INNER JOIN NESR_ONLINE.OFICINA oficina
                       ON (TICKETAUD.OFICINA_ID = oficina.OFICINA_ID)
                    LEFT JOIN NESR_ONLINE.COLA_SERVICIO colaServicio
                       ON (TICKETAUD.COLA_SERVICIO_ID =
                              colaServicio.COLA_SERVICIO_ID)
                    LEFT JOIN NESR_ONLINE.COLA cola
                       ON (colaServicio.COLA_ID = cola.COLA_ID)
                    LEFT JOIN NESR_ONLINE.SERVICIO servicio
                       ON (colaServicio.SERVICIO_ID = servicio.SERVICIO_ID)
                    LEFT JOIN NESR_ONLINE.PUESTO puesto
                       ON (TICKETAUD.PUESTO_ID = puesto.PUESTO_ID)
                    LEFT JOIN NESR_ONLINE.MOTIVO_VISITA
                       ON (TICKETAUD.MOTIVO_VISITA_ID =
                              MOTIVO_VISITA.MOTIVO_VISITA_ID)
              WHERE     TRUNC (TICKETAUD.FECHA_CREACION) = TRUNC (fechaHoy)
                    AND (   (    TICKETAUD.ESTADO IN
                                    ('ATENDIDO', 'FINALIZADO')
                             AND TICKETAUD.TICKET_ID IN
                                    (SELECT DISTINCT TA1.TICKET_ID
                                       FROM NESR_ONLINE.TICKET_AUD TA1
                                      WHERE     TA1.TICKET_ID =
                                                   TICKETAUD.TICKET_ID
                                            AND TA1.REV > TICKETAUD.REV
                                            AND TA1.ESTADO IN
                                                   ('ESPERANDO',
                                                    'FLOTANTE',
                                                    'FINALIZADO',
                                                    'ABANDONADO'))
                             AND TICKETAUD.REV =
                                    (SELECT MAX (TA2.REV)
                                       FROM NESR_ONLINE.TICKET_AUD TA2
                                      WHERE     TA2.TICKET_ID =
                                                   TICKETAUD.TICKET_ID
                                            AND TA2.ESTADO IN
                                                   ('ESPERANDO',
                                                    'ATENDIDO',
                                                    'FINALIZADO',
                                                    'ABANDONADO')
                                            AND TA2.REV <
                                                   (SELECT MIN (TA3.REV)
                                                      FROM NESR_ONLINE.TICKET_AUD TA3
                                                     WHERE     TA3.TICKET_ID =
                                                                  TICKETAUD.TICKET_ID
                                                           AND TA3.REV >
                                                                  TICKETAUD.REV
                                                           AND TA3.ESTADO IN
                                                                  ('ESPERANDO',
                                                                   'FLOTANTE',
                                                                   'FINALIZADO',
                                                                   'ABANDONADO'))))
                         OR (    TICKETAUD.ESTADO IN ('ABANDONADO')
                             AND TICKETAUD.MOTIVO_ABANDONO_ID = 124)))
      LOOP
         contador := contador + 1;
         tiempo_espera :=
              TRUNC (registro.fecha)
            + (registro.fecha_llamado - registro.fecha_ingreso);
         tiempo_atencion :=
              TRUNC (registro.fecha)
            + (registro.fecha_fin_atencion - registro.fecha_llamado);

         IF (UPPER (registro.tipo_cola) = 'PRINCIPAL')
         THEN
            tiempo_espera := tiempo_espera - (registro.delayEmision / 86400);
         ELSIF (UPPER (registro.tipo_cola) = 'DERIVADO')
         THEN
            tiempo_espera := tiempo_espera - (registro.delayDerivacion / 1440);
         ELSIF (UPPER (registro.tipo_cola) = 'FLOTANTE')
         THEN
            registro.numero_servicio := '0';
            registro.nombre_servicio := '';
            registro.t_obj_espera := '';
            registro.t_max_espera := '';
         END IF;

         UTL_FILE.PUT_LINE (
            file,
               TO_CHAR (registro.fecha, 'YYYYMMDD')
            || ';'
            || SUBSTR (
                     longitudAux
                  || TO_NUMBER (REGEXP_SUBSTR (registro.sucursal, '^\d+')),
                  -4,
                  4)
            || ';'
            || SUBSTR (longitudAux || registro.zona, -4, 4)
            || ';'
            || SUBSTR (registro.tipo_cola || longitudAux, 1, 9)
            || ';'
            || SUBSTR (registro.ticket, 1, 5)
            || ';'
            || SUBSTR (longitudAux || registro.numero_servicio, -5, 5)
            || ';'
            || SUBSTR (registro.nombre_servicio || longitudAux, 1, 50)
            || ';'
            || SUBSTR (longitudAux || registro.numero_puesto, -3, 3)
            || ';'
            || SUBSTR (registro.nombre_puesto || longitudAux, 1, 50)
            || ';'
            || SUBSTR (registro.usuario || longitudAux, 1, 50)
            || ';'
            || SUBSTR (
                     longitudAux
                  || TO_CHAR (
                          TO_DATE ('00:00:00', 'HH24:MI:SS')
                        + registro.t_obj_espera / 1440,
                        'HH24:MI:SS'),
                  -8,
                  8)
            || ';'
            || SUBSTR (
                     longitudAux
                  || TO_CHAR (
                          TO_DATE ('00:00:00', 'HH24:MI:SS')
                        + registro.t_max_espera / 1440,
                        'HH24:MI:SS'),
                  -8,
                  8)
            || ';'
            || TO_CHAR (registro.fecha_ingreso, 'HH24:MI:SS')
            || ';'
            || TO_CHAR (registro.fecha_llamado, 'HH24:MI:SS')
            || ';'
            || TO_CHAR (registro.fecha_fin_atencion, 'HH24:MI:SS')
            || ';'
            || TO_CHAR (tiempo_espera, 'HH24:MI:SS')
            || ';'
            || TO_CHAR (tiempo_atencion, 'HH24:MI:SS')
            || ';'
            || SUBSTR (longitudAux, 1, 4)
            || ';'
            || SUBSTR (longitudAux, 1, 50)
            || ';'
            || SUBSTR (registro.cod_motivo || longitudAux, 1, 4)
            || ';'
            || SUBSTR (registro.nom_motivo || longitudAux, 1, 50)
            || ';'
            || SUBSTR (registro.ESTADO || longitudAux, 1, 25)
            || ';'
            || SUBSTR (registro.MOTIVO_ABANDONO || longitudAux, 1, 50)
            || ';'
            || SUBSTR (registro.TECN || longitudAux, 1, 10));
      END LOOP;

      UTL_FILE.FCLOSE (file);
      DBMS_OUTPUT.put_line (
         'Exportar datos ticket cliente finalizo con exito!');
      DBMS_OUTPUT.put_line ('Fecha ingresada: ' || fechaHoy);
      DBMS_OUTPUT.put_line ('Tickets Clientes encontrados: ' || contador);
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (
            'Error: ' || SQLCODE || ', detalle: ' || SQLERRM);
         resultado := 1;
   END EXPORTAR_DATOS_TICKET_CLIENTE