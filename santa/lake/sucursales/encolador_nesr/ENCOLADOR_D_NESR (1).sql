 PROCEDURE EXPORTAR_DATOS_TICKET_RESUMIDO (direccion           VARCHAR2,
                                             nombreArchivo       VARCHAR2,
                                             fecha               VARCHAR2,
                                             extension           VARCHAR2,
                                             resultado       OUT NUMBER)
   IS
      file               UTL_FILE.FILE_TYPE;
      productoSegmento   VARCHAR2 (200);
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
            'ENCOLADOR_D_NESR_' || TO_CHAR (fechaHoy, 'YYYYMMDD');
      ELSE
         nombreArchivoAux := nombreArchivo;
      END IF;

      file :=
         UTL_FILE.FOPEN (direccion,
                         nombreArchivoAux || '.' || extension,
                         'W');

      FOR registro
         IN (SELECT t.LETRA || LPAD (t.NUMERO, 3, 0) id_tkt,
                    NVL2 (o.oficina_depende_id,
                          (SELECT numero
                             FROM oficina
                            WHERE oficina_id = o.oficina_depende_id),
                          o.numero)
                       id_suc,
                    cd.nombre sector_espera,
                    c.APELLIDO apellidoCliente,
                    c.NOMBRE nombreCliente,
                    '' cod_producto,
                    '' cod_motivo,
                    '' contacto,
                    c.TIPO_DOCUMENTO tipodoc,
                    c.NRO_DOCUMENTO nrodoc,
                    c.NUP nup,
                    c.OTROS atributosCliente,
                    t.FECHA_CREACION fechaticket,
                    s.NUMERO numero_servicio,
                    'PE' canalcomp,
                    t.LEGAJO_OPERADOR oficial_atencion,
                    '' id_gestion,
                    SYSDATE fecha_proceso,
                    'N' procesado,
                    CASE cola.nombre
                       WHEN 'Derivados'
                       THEN
                          NULL
                       ELSE
                          (SELECT MAX (t2.FECHA_MODIFICACION)
                             FROM TICKET_AUD t2
                            WHERE     t2.ESTADO = 'ATENDIDO'
                                  AND t2.REV <= t.REV
                                  AND t2.ticket_id = t.ticket_id
                                  AND '0' =
                                         (SELECT COUNT (ticket_id)
                                            FROM ticket_aud
                                           WHERE     ticket_id = t.ticket_id
                                                 AND estado = 'FLOTANTE'))
                    END
                       fecha_atencion,
                    CASE t.ESTADO
                       WHEN 'ESPERANDO'
                       THEN
                          NULL
                       WHEN 'ATENDIDO'
                       THEN
                          (SELECT MAX (t2.FECHA_MODIFICACION)
                             FROM TICKET_AUD t2
                            WHERE     t2.ESTADO = 'ATENDIDO'
                                  AND t2.REV <= t.REV
                                  AND t2.ticket_id = t.ticket_id
                                  AND '0' =
                                         (SELECT COUNT (ticket_id)
                                            FROM ticket_aud
                                           WHERE     ticket_id = t.ticket_id
                                                 AND estado = 'FLOTANTE'))
                       WHEN 'FINALIZADO'
                       THEN
                          (SELECT MAX (t2.FECHA_MODIFICACION)
                             FROM TICKET_AUD t2
                            WHERE     t2.ESTADO = 'FINALIZADO'
                                  AND t2.REV <= t.REV
                                  AND t2.ticket_id = t.ticket_id
                                  AND '0' =
                                         (SELECT COUNT (ticket_id)
                                            FROM ticket_aud
                                           WHERE     ticket_id = t.ticket_id
                                                 AND estado = 'FLOTANTE'))
                       ELSE
                          NULL
                    END
                       fecha_fin_atencion,
                    s.NOMBRE nombre_servicio,
                    mv.NOMBRE motivo_enc,
                    NVL2 (
                       t.NOMBRE_OPERADOR,
                          TRIM (t.APELLIDO_OPERADOR)
                       || ', '
                       || TRIM (t.NOMBRE_OPERADOR),
                       '')
                       oficial_nombre,
                    '' ORIGEN,
                    T.ESTADO,
                    ma.NOMBRE MOTIVO_ABANDONO,
                    (T.TECN * 100) TECN
               FROM TICKET_AUD t
                    LEFT JOIN CLIENTE c
                       ON t.TICKET_ID = c.TICKET_ID
                    LEFT JOIN COLA_SERVICIO_AUD cs
                       ON t.COLA_SERVICIO_ID = cs.COLA_SERVICIO_ID
                    LEFT JOIN COLA cola
                       ON cola.COLA_ID = cs.COLA_ID
                    LEFT JOIN SERVICIO s
                       ON cs.SERVICIO_ID = s.SERVICIO_ID
                    LEFT JOIN OFICINA o
                       ON t.OFICINA_ID = o.OFICINA_ID
                    LEFT JOIN CIRCUITO_DIRECCIONAMIENTO cd
                       ON cd.circuito_direccionamiento_id =
                             s.circuito_direccionamiento_id
                    LEFT JOIN MOTIVO_VISITA mv
                       ON t.MOTIVO_VISITA_ID = mv.MOTIVO_VISITA_ID
                    LEFT JOIN MOTIVO_ABANDONO ma
                       ON ma.MOTIVO_ABANDONO_ID = t.MOTIVO_ABANDONO_ID
              WHERE     cs.REV =
                           (SELECT MAX (cs2.REV)
                              FROM COLA_SERVICIO_AUD cs2
                             WHERE     cs2.REV <= t.REV
                                   AND cs2.cola_servicio_id =
                                          t.cola_servicio_id)
                    AND TRUNC (T.FECHA_CREACION) = TRUNC (fechaHoy)
                    AND (   t.ESTADO = 'FINALIZADO'
                         OR (    t.ESTADO = 'FLOTANTE'
                             AND t.rev = (SELECT MAX (ta4.rev)
                                            FROM ticket_aud ta4
                                           WHERE ta4.ticket_id = t.ticket_id))
                         OR (    t.ESTADO = 'ESPERANDO'
                             AND t.rev = (SELECT MAX (ta2.rev)
                                            FROM ticket_aud ta2
                                           WHERE ta2.ticket_id = t.ticket_id))
                         OR (    t.ESTADO = 'ATENDIDO'
                             AND t.rev = (SELECT MAX (ta3.rev)
                                            FROM ticket_aud ta3
                                           WHERE ta3.ticket_id = t.ticket_id))
                         OR (    t.ESTADO = 'ABANDONADO'
                             AND t.rev = (SELECT MAX (ta5.rev)
                                            FROM ticket_aud ta5
                                           WHERE ta5.ticket_id = t.ticket_id))))
      LOOP
         contador := contador + 1;
         productoSegmento :=
            OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                         'PRODUCTOSEGMENTOINDIVIDUO');

         IF (productoSegmento = '')
         THEN
            productoSegmento :=
               OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                            'PRODUCTOSEGMENTOEMPRESA');
         END IF;

         UTL_FILE.PUT_LINE (
            file,
               SUBSTR (registro.id_tkt, 1, 5)
            || ';'
            || SUBSTR (registro.id_suc, 1, 4)
            || ';'
            || SUBSTR (registro.sector_espera, 1, 26)
            || ';'
            || SUBSTR (registro.apellidoCliente, 1, 40)
            || ';'
            || SUBSTR (registro.nombreCliente, 1, 40)
            || ';'
            || SUBSTR (registro.cod_producto, 1, 4)
            || ';'
            || SUBSTR (registro.cod_motivo, 1, 3)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'MEJORPRODUCTO'),
                  1,
                  8)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'SEGMENTO'),
                  1,
                  2)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'SEMAFORORENTABILIDAD'),
                  1,
                  2)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'SEMAFORORENTA'),
                  1,
                  2)
            || ';'
            || SUBSTR (productoSegmento, 1, 3)
            || ';'
            || SUBSTR (registro.contacto, 1, 10)
            || ';'
            || SUBSTR (registro.tipodoc, 1, 1)
            || ';'
            || SUBSTR (registro.nrodoc, 1, 11)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'SUBSEGMENTO'),
                  1,
                  1)
            || ';'
            || SUBSTR (registro.nup, 1, 8)
            || ';'
            || SUBSTR (TO_CHAR (registro.fechaticket, 'YYYYMMDDHH24MISS'),
                       1,
                       14)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'CAMPANA'),
                  1,
                  40)
            || ';'
            || SUBSTR (registro.numero_servicio, 1, 5)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'TIPOATENCION'),
                  1,
                  1)
            || ';'
            || SUBSTR (
                  OBTENER_ATRIBUTO_POR_CODIGO (registro.atributosCliente,
                                               'LEGAJO'),
                  1,
                  8)
            || ';'
            || SUBSTR (registro.canalcomp, 1, 2)
            || ';'
            || SUBSTR (TO_CHAR (registro.fecha_atencion, 'YYYYMMDDHH24MISS'),
                       1,
                       14)
            || ';'
            || SUBSTR (registro.oficial_atencion, 1, 8)
            || ';'
            || SUBSTR (registro.id_gestion, 1, 4)
            || ';'
            || SUBSTR (TO_CHAR (registro.fecha_proceso, 'YYYYMMDDHH24MISS'),
                       1,
                       14)
            || ';'
            || SUBSTR (registro.procesado, 1, 1)
            || ';'
            || SUBSTR (
                  TO_CHAR (registro.fecha_fin_atencion, 'YYYYMMDDHH24MISS'),
                  1,
                  14)
            || ';'
            || SUBSTR (registro.nombre_servicio, 1, 50)
            || ';'
            || SUBSTR (registro.motivo_enc, 1, 50)
            || ';'
            || SUBSTR (registro.oficial_nombre, 1, 42)
            || ';'
            || SUBSTR (registro.ORIGEN, 1, 5)
            || ';'
            || SUBSTR (registro.ESTADO, 1, 25)
            || ';'
            || SUBSTR (registro.MOTIVO_ABANDONO, 1, 50)
            || ';'
            || SUBSTR (registro.TECN, 1, 10)
            || ';');
      END LOOP;

      UTL_FILE.FCLOSE (file);
      DBMS_OUTPUT.put_line (
         'Exportar datos ticket cliente resumido finalizo con exito!');
      DBMS_OUTPUT.put_line ('Fecha ingresada: ' || fechaHoy);
      DBMS_OUTPUT.put_line ('Tickets Clientes encontrados: ' || contador);
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (
            'Error: ' || SQLCODE || ', detalle: ' || SQLERRM);
         resultado := 1;
   END EXPORTAR_DATOS_TICKET_RESUMIDO;