      ***************************************************************** 00010000
      * NOMBRE ARCHIVO.......: XFTC310                                * 00020001
      * DESCRIPCION..........: MOV. LIQUIDADOS (NI CONSUMOS/ADELANTOS)* 00030001
      *                        MOV. REVERSADOS                        * 00031003
      * LONGITUD DE REGISTRO.: 150 CARACTERES                         * 00040012
      * ORGANIZACION.........:                                        * 00050000
      *                                                               * 00060000
      * CLAVES                                                        * 00070000
      * ------> PRINCIPAL....:                                        * 00080000
      * ------> ALTERNATIVA 1:                                        * 00090000
      ***************************************************************** 00100000
       01  WAFTC310.                                                    00120002
                                                                        00130000
           03  WAFTC310-MOV-LIQ-REV                         PIC X(150). 00140013
                                                                        00150000
      * HEADER DEL REGISTRO                                             00160000
      *********************                                             00170000
           03  WAFTC310-HEADER REDEFINES WAFTC310-MOV-LIQ-REV.          00180003
               05  WAFTC310-H-ENTIDAD                   PIC X(03).      00190003
               05  FILLER                               PIC X(02).      00200003
      * LEYENDA MOV. LIQ. = 'MOVIM. LIQUIDADOS NO SON CONS./ADEL. AL"   00201005
      * LEYENDA MOV. REV. = 'MOVIMIENTOS REVERSADOS AL"                 00202005
               05  WAFTC310-H-LEYENDA-1                 PIC X(40).      00210003
               05  WAFTC310-H-FEC-CORTE-AAAAMMDD        PIC 9(08).      00220006
               05  FILLER   REDEFINES WAFTC310-H-FEC-CORTE-AAAAMMDD.    00221006
                   07  WAFTC310-H-FEC-CORTE-CENT        PIC 9(02).      00222006
                   07  WAFTC310-H-FEC-CORTE-AA          PIC 9(02).      00222106
                   07  WAFTC310-H-FEC-CORTE-MM          PIC 9(02).      00223006
                   07  WAFTC310-H-FEC-CORTE-DD          PIC 9(02).      00224006
      * LEYENDA 2 = "PROCESADO"                                         00225006
               05  WAFTC310-H-LEYENDA-2                 PIC X(10).      00230003
               05  WAFTC310-H-FEC-PROC-AAAAMMDD         PIC 9(08).      00231006
               05  FILLER   REDEFINES WAFTC310-H-FEC-PROC-AAAAMMDD.     00232006
                   07  WAFTC310-H-FEC-PROC-CENT         PIC 9(02).      00233006
                   07  WAFTC310-H-FEC-PROC-AA           PIC 9(02).      00234006
                   07  WAFTC310-H-FEC-PROC-MM           PIC 9(02).      00235006
                   07  WAFTC310-H-FEC-PROC-DD           PIC 9(02).      00236006
               05  FILLER                               PIC X(76).      00240012
      * TIPO DE REGISTRO = "20"                                         00240106
               05  WAFTC310-H-TIPO-REG                  PIC X(02).      00241003
               05  FILLER                               PIC X(01).      00242003
                                                                        00250000
           03  WAFTC310-REG-DATOS REDEFINES WAFTC310-MOV-LIQ-REV.       00260006
                                                                        00270000
      * COD DE BANCO                                                    00271001
               05  WAFTC310-ENTIDAD                 PIC 9(03).          00272004
                                                                        00273001
      * CODIGO DE CASA                                                  00274001
               05  WAFTC310-COD-SUCU                PIC 9(03).          00275001
                                                                        00276001
      * CODIGO DE OPERACION                                             00277001
               05  WAFTC310-COD-OPE                 PIC 9(04).          00278001
                                                                        00279001
      * NUMERO DE CUENTA                                                00279110
               05  WAFTC310-NRO-CTA                 PIC 9(10).          00279210
                                                                        00279301
      * NUMERO DE TARJETA                                               00279401
               05  WAFTC310-NRO-TARJ                PIC 9(16).          00279501
                                                                        00279601
      * FECHA DE PRESENTACION (DDMMAA)                                  00279701
               05  WAFTC310-FEC-PRES-AAMMDD          PIC 9(06).         00279807
               05  FILLER   REDEFINES WAFTC310-FEC-PRES-AAMMDD.         00279907
                   07  WAFTC310-FEC-PRES-AA          PIC 9(02).         00280107
                   07  WAFTC310-FEC-PRES-MM          PIC 9(02).         00280207
                   07  WAFTC310-FEC-PRES-DD          PIC 9(02).         00280307
                                                                        00280401
      * FECHA DE ORIGEN (DDMMAA)                                        00280501
               05  WAFTC310-FEC-ORIGEN-AAMMDD       PIC 9(06).          00280609
               05  FILLER   REDEFINES WAFTC310-FEC-ORIGEN-AAMMDD.       00280707
                   07  WAFTC310-FEC-ORIGEN-AA        PIC 9(02).         00280807
                   07  WAFTC310-FEC-ORIGEN-MM        PIC 9(02).         00280907
                   07  WAFTC310-FEC-ORIGEN-DD        PIC 9(02).         00281007
                                                                        00281101
      * NUMERO DE COMPROBANTE                                           00281201
               05  WAFTC310-NRO-COMP                PIC 9(08).          00281301
                                                                        00281401
      * MONEDA                                                          00282001
               05  WAFTC310-MONEDA                  PIC X(03).          00290001
                   88  WAFTC310-PES              VALUE 'ARP'.           00300001
                   88  WAFTC310-DOL              VALUE 'USD'.           00310001
                                                                        00320000
      * IMPORTE                                                         00321101
               05  WAFTC310-IMPORTE                 PIC 9(11)V99.       00322011
                                                                        00322111
      * SIGNO                                                           00322211
               05  WAFTC310-SIGNO                   PIC X.              00322311
                                                                        00322411
      * GRUPO DE AFINIDAD                                               00323001
               05  WAFTC310-GRP-AFI                 PIC 9(04).          00324001
                                                                        00325000
      * CARTERA                                                         00325101
               05  WAFTC310-CARTERA                 PIC 9(02).          00325201
                                                                        00325301
      * NUMERO DE ESTABLECIMIENTO                                       00325401
               05  WAFTC310-NRO-EST                 PIC 9(10).          00325501
                                                                        00325601
      * ORIGEN                                                          00325701
               05  WAFTC310-ORIGEN                  PIC X(01).          00325801
                                                                        00325901
      * INDICADOR DE REVERSADO                                          00326001
      * SI ES "R" ES MOV. REVERSADO Y SI ES BLANCO ES MOV. LIQ.         00326107
               05  WAFTC310-REVERSO                 PIC X(01).          00326201
                                                                        00326312
      * IMPORTE APLICADO EN PESOS                                       00326412
               05  WAFTC310-IMPORTE-ARP             PIC 9(11)V99.       00326512
                                                                        00326612
      * SIGNO IMPORTE APLICADO EN PESOS                                 00326812
               05  WAFTC310-SIGNO-ARP               PIC X.              00326912
                                                                        00327012
      * IMPORTE APLICADO EN DOLARES                                     00327112
               05  WAFTC310-IMPORTE-USD             PIC 9(11)V99.       00327212
                                                                        00327312
      * SIGNO IMPORTE APLICADO EN DOLARES                               00327412
               05  WAFTC310-SIGNO-USD               PIC X.              00327512
                                                                        00327612
      * MARCA AGRO                                                      00327701
  MDS          05  WAFTC310-MK-AGRO                 PIC X.              00327702
                                                                        00327703
      * FILLER                                                          00327704
      *        05  FILLER                           PIC X(28).          00328012
               05  FILLER                           PIC X(27).          00328013
                                                                        00370000
      * TIPO DE REGISTRO = '30'                                         00370112
               05  WAFTC310-TPO-REG                 PIC X(2).           00370212
                                                                        00370312
      * FINAL (FIJO "*")                                                00370412
               05  WAFTC310-MK-FINAL                PIC X(01).          00370512
                                                                        00990000
      * TRAILER DEL REGISTRO                                            01690000
      *********************                                             01700000
           03  WAFTC310-TRAILER REDEFINES WAFTC310-MOV-LIQ-REV.         01710008
               05  WAFTC310-T-ENTIDAD                   PIC 9(03).      01720007
               05  FILLER                               PIC X(12).      01721007
               05  WAFTC310-T-CANT-REG                  PIC 9(08).      01740007
               05  WAFTC310-T-IMP-TOT-PES               PIC 9(15)V99.   01760007
               05  FILLER                               PIC X(107).     01780012
      * TIPO DE REGISTRO = "40"                                         01790007
               05  WAFTC310-T-TIPO-REG                  PIC X(02).      01811007
               05  FILLER                               PIC X(01).      01820007
