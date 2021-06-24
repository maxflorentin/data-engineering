      ******************************************************************
      *                                                                *
      * COPY    : ZOECCRE                                              *
      *                                                                *
      * PREFIJO : :ZOECCRE:                                            *
      *                                                                *
      * OBJETIVO: COPY DEL ARCHIVO DE TARJETAS ELECTRON SIN CUENTA     *
      *           (CREDENCIALES).                                      *
      *                                                                *
      * LONGITUD: 481 BYTES                                            *
      *                                                                *
      ******************************************************************
      * LA CODIFICACION DE DOCUMENTO, DOMICILIO, ETC RESPONDE A ALTAIR *
      ******************************************************************
GAMA01* 06/08/2011 MODIFICACIONES POR ALTA DE TARJETA VIRTUAL          *
      ******************************************************************
MOD001* 06/08/2014 MODIFICACION POR CAMBIO DE ROLES                    *
      ******************************************************************

       02 :ZOECCRE:-REGISTRO.
      *
      *----------------------------------------------------------------*
      * CLAVES DE CREDENCIALES                                         *
      *----------------------------------------------------------------*
          05 :ZOECCRE:-DATOS-CREDENCIAL.
      * CLAVE PRINCIPAL                                                *
             10 :ZOECCRE:-NUMERO-TARJETA      PIC X(19).

      * CLAVE ALTERNATIVA CON DUPLICADOS                               *
             10 :ZOECCRE:-DOCUMENTO.
                15 :ZOECCRE:-PETIPDOC         PIC X(02).
                 88 :ZOECCRE:-PETIPDOC-VALIDO     VALUE 'E' 'C' 'N'
                                                        'P' 'I' 'X'.
      *        E    LIBRETA ENROLAMIENTO
      *        C    LIBRETA CÌVICA
      *        N    D.N.I
      *        P    PASAPORTE
      *        I    CÈDULA IDENTIDAD
      *        X    D.N.I EXTRANJEROS

                15 :ZOECCRE:-PENUMDOC         PIC X(11).

      *----------------------------------------------------------------*
      * DATOS DE LA CREDENCIAL                                         *
      *----------------------------------------------------------------*
             10 :ZOECCRE:-ESTADO-TARJETA      PIC 9(01).
                88 :ZOECCRE:-ESTADO-VALIDO        VALUE 1 9.
                88 :ZOECCRE:-ACTIVA               VALUE 1.
                88 :ZOECCRE:-CERRADA              VALUE 9.

             10 :ZOECCRE:-MARCA-TAR           PIC X(01).
GAMA01*         88 :ZOECCRE:-MARCA-VALIDA         VALUE '*'.
                88 :ZOECCRE:-MARCA-VALIDA         VALUE '*' 'V'.
                88 :ZOECCRE:-ELECTRON             VALUE '*'.

             10 :ZOECCRE:-TIPO-PLAS           PIC X(03).                 0163000

             10 :ZOECCRE:-COD-COMERCIAL       PIC X(01).                 0163000
GAMA01*         88 :ZOECCRE:-COD-COMERCIAL-VALIDO VALUE 'I' 'U'.
GAMA01          88 :ZOECCRE:-COD-COMERCIAL-VALIDO VALUE 'I' 'U' 'C'.

             10 :ZOECCRE:-COD-EMISION         PIC X(03).                 0163000
             10 :ZOECCRE:-NOMBRE-FOTO         PIC X(10).
             10 :ZOECCRE:-COD-DIST            PIC X(02).
             10 :ZOECCRE:-TRACKI              PIC X(16).                 0163000

             10 :ZOECCRE:-TRACKII             PIC X(10).                 0163000
             10 :ZOECCRE:-CUARTA-LINEA        PIC X(24).                 0163000

             10 :ZOECCRE:-PEVALIND            PIC X(01).                 0163000
MOD001        88 :ZOECCRE:-PEVALIND-VALIDO    VALUE  'A' 'B' 'D' 'G'
MOD001                                               'F' 'I' 'X' 'Z'.
MOD001*    A=ALUMNO   => 18 <= 21 A—OS
MOD001*    B=ALUMNO   => 22 <= 24 A—OS
      *    D=DOCENTE
MOD001*    G=GRADUADO => 25 <= 31 A—OS
      *    F=NO DOCENTE
MOD001*    I=UNIVERSITARIO
MOD001*    X=PERI OUT >= 32 A—OS
MOD001*    Z=OTROS    <  18 A—OS

      * FECHA DE ALTA (AAAAMMDD)
             10 :ZOECCRE:-FEC-ALTA            PIC 9(08).
             10 :ZOECCRE:-USUARIO-ALTA        PIC X(08).

      * FECHA DE ULTIMA MODIFICACION
             10 :ZOECCRE:-FEC-ULT-ACT         PIC 9(08).
             10 :ZOECCRE:-USUARIO-ACT         PIC X(08).

      * FECHA DE ULTIMO EMBOZADO
             10 :ZOECCRE:-FEC-EMBZ            PIC 9(08).

             10 :ZOECCRE:-COD-REEM            PIC X(01).
                88 :ZOECCRE:-NO-REEMITIR          VALUE '0'.
                88 :ZOECCRE:-REEMITIR-IGUAL-VTO   VALUE '1'.
                88 :ZOECCRE:-REEMITIR-NUEVO-VTO   VALUE '2'.

             10 :ZOECCRE:-MOT-EMISION         PIC X(01).
                88 :ZOECCRE:-EMISION-ALTA         VALUE '1'.
                88 :ZOECCRE:-EMISION-RENOVACION   VALUE '2'.
                88 :ZOECCRE:-EMISION-REEMBOZADO   VALUE '3'.
                88 :ZOECCRE:-EMISION-RETENCION    VALUE '4'.

             10 :ZOECCRE:-FEC-ENVIO-BANELCO   PIC 9(08).

      * FECHA EN QUE FUE PROCESADA LA NOVEDAD EN BANELCO
             10 :ZOECCRE:-FEC-ACT-BANELCO     PIC 9(08).
             10 :ZOECCRE:-RETORNO-BANELCO     PIC X(05).

      * FECHA DE OBTENCION DE NUP
             10 :ZOECCRE:-FEC-NUP             PIC 9(08).
             10 :ZOECCRE:-USUARIO-NUP         PIC X(08).

             10 :ZOECCRE:-NRO-TAR-ANT         PIC X(19).

      * VTO=AAAAMM
             10 :ZOECCRE:-VTO                 PIC 9(06).

             10 :ZOECCRE:-SUCURSAL-ADM        PIC X(04).

      *----------------------------------------------------------------*
      * DATOS DE LA PERSONA                                            *
      *----------------------------------------------------------------*
          05 :ZOECCRE:-PENUMPER               PIC X(08).

          05 :ZOECCRE:-DATOS-PERSONA.
             10 :ZOECCRE:-PEPRIAPE            PIC X(20).
             10 :ZOECCRE:-PENOMPER            PIC X(40).

             10 :ZOECCRE:-PESEXPER            PIC X(01).
              88 :ZOECCRE:-PESEXPER-VALIDO        VALUE  'H' 'M'.

             10 :ZOECCRE:-PEESTCIV            PIC X(01).
              88 :ZOECCRE:-PEESTCIV-VALIDO        VALUE  'S' 'C' 'D'
                                                         'V' 'E'.
      *    S    SOLTERO
      *    C    CASADO
      *    D    DIVORCIADO
      *    V    VIUDO
      *    E    SEPARADO

             10 :ZOECCRE:-PEFECNAC            PIC X(08).

      *----------------------------------------------------------------*
      * DATOS DEL DOMICILIO                                            *
      *----------------------------------------------------------------*
             10 :ZOECCRE:-PENOMCAL            PIC X(50).

             10 :ZOECCRE:-PENUMBLO.
                15 FILLER                     PIC X(05).
                15 :ZOECCRE:-PENUMBL2         PIC X(05).

             10 :ZOECCRE:-PEOBSPIS            PIC X(04).

             10 :ZOECCRE:-PEOBSDTO            PIC X(04).

             10 :ZOECCRE:-PEDESLOC            PIC X(30).

             10 :ZOECCRE:-PECODPOS.
                15 :ZOECCRE:-PATENTE          PIC X(01).
                15 :ZOECCRE:-CODPOST          PIC X(04).
                15 :ZOECCRE:-MANZANA          PIC X(03).

             10 :ZOECCRE:-PECODPRO            PIC X(02).

             10 :ZOECCRE:-PECODPAI            PIC X(03).

             10 :ZOECCRE:-PECARTEL            PIC X(04).

             10 :ZOECCRE:-PENUMTEL            PIC X(08).
                                                                        01630000
      *----------------------------------------------------------------*
          05 :ZOECCRE:-TIPO-TARJETA           PIC X(01).
             88 :ZOECCRE:-ADM                     VALUE 'A'.
             88 :ZOECCRE:-DEBITO                  VALUE 'P'.
                                                                        01630000
          05 FILLER                           PIC X(67).                   00150
                                                                        01630000
