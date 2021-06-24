
      ********************************************************* ********
      *                          B S C H                               *
      *
      *          NOMBRE DE LA COPY : ZOECCUE (EX WATBE02)
      *          PREFIJO : ZOECCUE
      *          LONGITUD : 120 BYTES                                  *
      *          CLAVE : 45 BYTES (CUENTA-TARJETA).                    *
      *          CLAVE ALTERNATIVA: NRO-TARJETA   .                    *
      *          DESCRIPCION: MAESTRO DE cuentas BANELCO               *
      *
      ******************************************************************
      * COMPILAR CON COBOL II
      * EL PREFIJO DEBE SER REEMPLAZADO POR UN COPY REPLACING
      * EJ.: COPY ZOECCUE REPLACING ==:ZOECCUE:== BY ==ZOECCUE==
      *---------------------------------------------------*
            03 :ZOECCUE:-REGISTRO.
      * CLAVE PRINCIPAL
              05 :ZOECCUE:-CLAVE.
                    08  :ZOECCUE:-CLAVE-CUENTA.
                      10  :ZOECCUE:-CLAVE-SIN-FIRMANTE.
      * Entidad de la cuenta
                        12 :ZOECCUE:-ENTIDAD    PIC 9(4).
      * Centro de la cuenta
                        12 :ZOECCUE:-CENTRO     PIC 9(4).
      * Cuenta
                        12 :ZOECCUE:-CUENTA.
                         15 :ZOECCUE:-CUENTA-PROD PIC 9(03).
                         15 :ZOECCUE:-CUENTA-NRO  PIC 9(09).
      * Divisa de la cuenta
                        12 :ZOECCUE:-DIVISA     PIC X(3).
      * Firmante de la cuenta
                      10 :ZOECCUE:-FIRMANTE   PIC 9(03).
      * Numero de tarjeta completo -  CLAVE ALTERNATIVA
                    08  :ZOECCUE:-NRO-TARJETA   PIC X(19).
      * Tipo de cuenta en formato Banelco
            05 :ZOECCUE:-TIPO-CTA-BANELCO       PIC 9(02).
                        88 :ZOECCUE:-CC-PESOS VALUE 01.
                        88 :ZOECCUE:-CC-DOLAR VALUE 02.
                        88 :ZOECCUE:-CA-PESOS VALUE 11.
                        88 :ZOECCUE:-CA-DOLAR VALUE 12.
              05 :ZOECCUE:-ESTADO               PIC 9.
                        88 :ZOECCUE:-ABIERTA    VALUE 1.
                        88 :ZOECCUE:-PRINCIPAL  VALUE 3.
                        88 :ZOECCUE:-DEPOSITO   VALUE 4.
                        88 :ZOECCUE:-CERRADA    VALUE 9.
              05 :ZOECCUE:-PLAN-SUELDO          PIC X.
              05 :ZOECCUE:-FAST-CASH            PIC X.
              05 :ZOECCUE:-SUSCRIPTOR           PIC X(11).
      * FECHA DE VINCULACION
              05 :ZOECCUE:-FEC-ALTA             PIC 9(08).
      * USUARIO QUE VINCULO
              05 :ZOECCUE:-USUARIO-ALTA         PIC X(08).
      * FECHA ULTIMO CAMBIO
              05 :ZOECCUE:-FEC-ULT-ACT          PIC 9(08).
      * USUARIO ULTIMO CAMBIO
              05 :ZOECCUE:-USUARIO-ULT-ACT      PIC X(08).
      * MARCA DE VINCULACION
              05 :ZOECCUE:-MARCA-VINCULA        PIC X(02).
      *ISBAN-0001-I
              05 :ZOECCUE:-CTA-PREFERIDA        PIC X(01).
                    88 :ZOECCUE:-CTA-PREFERIDA-SI  VALUE 'P'.
                    88 :ZOECCUE:-CTA-PREFERIDA-NO  VALUE 'N'.
              05 FILLER                         PIC X(24).
      *       05 FILLER                         PIC X(25).
      *ISBAN-0001-F
      *
