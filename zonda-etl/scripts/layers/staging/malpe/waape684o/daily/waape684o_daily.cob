******************************************************************
      * PROYECTO: OBTENCION DE CLIENTES MIPYME                         *
      * NOMBRE DEL OBJETO:  WAAPE684                                   *
      *                                                                *
      * CONSOLIDADO DE INFORMACION DE SGE MAS PEE                      *
      *                                                                *
      * LONGITUD TOTAL DEL REGISTRO EN BYTES :     370                 *
      *                                                                *
      ******************************************************************
      * MODIFICACION     : ID4530                                      *
      * PETICION         : ID4530                                      *
      * AUTOR            : RUBEN SANCHEZ                               *
      * FECHA            : 08-08-2013                                  *
      * DESCRIPCION      : INCORPORAR EL CAMPO DEUDA                   *
      ******************************************************************
      * MODIFICACION     : MIPYME                                      *
      * PETICION         : MIPYME                                      *
      * AUTOR            : GERARDO PALLARES                            *
      * FECHA            : 02-07-2020                                  *
      * DESCRIPCION      : INCORPORAR EL CAMPO WAAPEPYM-TIPO-ACCION    *
      *                  : ESTE CAMPO INDICA SI EL CLIENTE SE INCLUYE, *
      *                  : SE EXCLUYE DEL PROCESO O NO SE HACE NADA    *
      ******************************************************************
      ******************************************************************
       03 WAAPE683-REG-CPA.
          05 WAAPEPYM-PENUMPER             PIC X(08).
          05 WAAPEPYM-TIPDOC               PIC X(02).
          05 WAAPEPYM-DOCUM                PIC X(11).
          05 WAAPEPYM-APEYNOMB             PIC X(60).
          05 WAAPEPYM-PETIPPER             PIC X(01).
             88 W683-ES-PJ                              VALUE 'J'.
             88 W683-ES-PF                              VALUE 'F'.
          05 WAAPEPYM-PESEGCAL             PIC X(03).
             88 W683-SEG-INDIVIDUO         VALUE 'A  ' 'B  '
                                                 'C  ' 'S  '.
             88 W683-SEG-P1-C1             VALUE 'P1 ' 'C1 '.
      *   ------------------------------------------------ HASTA AQUI:85
          05 WAAPEPYM-BAL-MONT-FEC.
             10 WAAPEPYM-MONT-BAL1         PIC S9(13)V9(02).
             10 WAAPEPYM-MONT-BAL1-X REDEFINES
                WAAPEPYM-MONT-BAL1         PIC X(15).
             10 WAAPEPYM-FEC-BAL1          PIC 9(08).
             10 WAAPEPYM-MONT-BAL2         PIC S9(13)V9(02).
             10 WAAPEPYM-MONT-BAL2-X REDEFINES
                WAAPEPYM-MONT-BAL2         PIC X(15).
             10 WAAPEPYM-FEC-BAL2          PIC 9(08).
             10 WAAPEPYM-MONT-BAL3         PIC S9(13)V9(02).
             10 WAAPEPYM-MONT-BAL3-X REDEFINES
                WAAPEPYM-MONT-BAL3         PIC X(15).
             10 WAAPEPYM-FEC-BAL3          PIC 9(08).
      *   ----------------------------------------------- HASTA AQUI:154
          05 WAAPEPYM-CLANAE10             PIC X(03).
          05 WAAPEPYM-PERESIVA             PIC X(02).
             88 W683-RESP-INSCRIPTO                    VALUE '05' '06'.
             88 W683-MONOTRIBUTISTA                    VALUE '02'.
          05 WAAPEPYM-MARCA-SEPYME         PIC X(01).
             88 W683-TIENE-SEPYME                      VALUE 'S'.
          05 WAAPEPYM-MARCA-5319           PIC X(03).
             88 W683-TIENE-NMP                         VALUE 'NMP'.
             88 W683-TIENE-MP                          VALUE 'MP '.
          05 WAAPEPYM-COD-MIPYME           PIC X(02).
          05 WAAPEPYM-FECHA-PROC           PIC X(10).
      *   ----------------------------------------------- HASTA AQUI:175
          05 WAAPEPYM-ORIGEN-REG           PIC X(03).
          05 WAAPEPYM-MARCA-BYP            PIC X(01).
             88 W683-TIENE-BIANDPI                     VALUE 'S'.
          05 WAAPEPYM-MOTIVO-OK            PIC X(01).
          05 WAAPEPYM-TIP-DOC-ADSF         PIC X(02).
          05 WAAPEPYM-FEC-INSC             PIC X(10).
          05 WAAPEPYM-PENUMGRU             PIC 9(08).
          05 WAAPEPYM-SECTOR               PIC X(02).
          05 WAAPEPYM-TAM-EMPRE            PIC X(02).
          05 WAAPEPYM-COD-MIPYME-REAL      PIC X(02).
          05 WAAPEPYM-USO-FUTURO           PIC X(13).
      *   ----------------------------------------------- HASTA AQUI:219
       03 WAAPE684-REG-CPA.
          05 WAAPEPYM-PECONPER             PIC X(03).
          05 WAAPEPYM-O-LETRA              PIC X(01).
          05 WAAPEPYM-PECODACT             PIC X(08).
          05 WAAPEPYM-CANTIDAD-BAL         PIC  9.
          05 WAAPEPYM-VTA-TOT-ANUAL        PIC S9(13).
      *   ----------------------------------------------- HASTA AQUI:245
          05 WAAPEPYM-OBS-REPETIDO         PIC X(08).
          05 WAAPEPYM-OBS-CLANAE           PIC X(30).
          05 WAAPEPYM-OBS-DESTINO          PIC X(24).
          05 WAAPEPYM-TIPO-ERROR           PIC X(01).
          05 WAAPEPYM-TIPO-ACCION          PIC X(01).
             88 W684-SIN-ACCION                     VALUE SPACES.
             88 W684-CLIENTE-INC                    VALUE 'I'.
             88 W684-CLIENTE-EXC                    VALUE 'E'.
          05 WAAPEPYM-FILLER               PIC X(03).
      * CUALQUIER VALOR EN ITEM 1,2,3 INDICA ERROR EN BAL1/BAL2/BAL3
      * G EN ITEM(X) = ANTIGUEDAD DE FECHA BALANCE1/2/3 > 18 MESES
      * D EN ITEM(X) = FECHA INVALIDA EN FECHA BALANCE1/2/3
      * F EN ITEM(X) = ERROR DE FORMATO EN VALOR BAL1/BAL2/BAL3
      * Z EN ITEM(X) = CEROS EN VALOR BAL1/BAL2/BAL3
          05 WAAPEPYM-IND-ERR-BAL.
             07 WAAPEPYM-ERR-BAL1          PIC X(01).
                88 W684-SIN-ERR-BAL1                   VALUE SPACES.
                88 W684-ANT-ERR-BAL1                   VALUE 'G'.
             07 WAAPEPYM-ERR-BAL2          PIC X(01).
             07 WAAPEPYM-ERR-BAL3          PIC X(01).
          05 FILLER REDEFINES WAAPEPYM-IND-ERR-BAL.
             07 WAAPEPYM-ERR-BAL           PIC X(01) OCCURS 3 TIMES.
ID4530*   05 WAAPEPYM-OBS-BALANCE          PIC X(55).
          05 WAAPEPYM-OBS-BALANCE          PIC X(38).
             88 W684-SIN-ERR-BALANCE                   VALUE SPACES.
ID4530    05 WAAPEPYM-DEUDA-TOT-CLI        PIC 9(15)V99.
      *   ----------------------------------------------- HASTA AQUI:370
      *
      ******************************************************************
      *                        F  I  N                                 *
      ******************************************************************