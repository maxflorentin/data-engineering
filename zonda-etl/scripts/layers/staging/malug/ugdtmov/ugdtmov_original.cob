000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* ÁREA       - ACTIVO                                            *00000400
000500* APLICACIÓN - PRÉSTAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCMOV                                    *00000700
000800* FECHA DE CREACIÓN - 24-NOV-1999                                *00000800
000900*                                                                *00000900
001000* Descripción del registro                                       *00001000
001100*                                                                *00001100
001200******************************************************************00001200
001300*                     LOG DE MODIFICACIONES                      *00001300
001400* -------------------------------------------------------------- *00001400
001500* FECHA    | AUTOR     | DESCRIPCIÓN                             *00001500
001600* -------------------------------------------------------------- *00001600
001700*                                                                *00001700
001800******************************************************************00001800
001900*                                                                *00001900
002000* CAMPO                      DESCRIPCIóN                         *00002000
002100* -------------------------- ----------------------------------- *00002100
002200* CUENTA                     NUMERO DE CUENTA                    *00002200
002300* OFICINA                    OFICINA                             *00002300
002400* ENTIDAD                    ENTIDAD                             *00002400
002500* NIO                        NUMERO INTERNO DE OPERACION         *00002500
002600* CODCONLI                   CLAVE DE CONCEPTO                   *00002600
002610* NUM-SECUENCIA              numero de secuencia                 *00002610
002700* COD-EVENTO                 CODIGO DE EVENTO                    *00002700
002710* TIPO-MOV                   TIPO DE MOVIMIENTO                  *00002710
002800* FEOPER                     FECHA OPERACION DEL MOVIMIENTO      *00002800
002900* FECONTA                    FECHA CONTABLE DEL MOVIMIENTO       *00002900
003000* FEVALOR                    FECHA VALOR DEL MOVIMIENTO          *00003000
003100* FELIQ                      FECHA LIQUIDACION SUBVENCION        *00003100
003200* CLAOPER                    CLAVE OPERACION                     *00003200
003300*                            34 - ABONO                          *00003300
003400*                            36 - CARGO                          *00003400
003500* ENTIOPE                    ENTIDAD OPERACION                   *00003500
003600* OFIOPE                     OFICINA OPERACION                   *00003600
003700* IMPMOVI                    IMPORTE DE MOVIMIENTO               *00003700
003800* COD-DIVISA                 CODIGO DE DIVISA                    *00003800
003900* TIP-CAMBIO-OPE             CAMBIO DE LA MONEDA A FECHA DE MOV. *00003900
004000* SALREAL                    SALDO REAL CUENTA PREVIO AL MOVIM.  *00004000
004400* UGYINCOR                   INDICADOR DE MOVIMIENTO CORRECCION  *00004400
004500*                            0 - NO ES MOVIMIENTO CORRECCION     *00004500
004600*                            1 - ES MOVIMIENTO CORRECCION        *00004600
004700* UGYINDIF                   INDICADOR DE MOVIMIENTO DIFERENCIAS *00004700
004800*                            0 - NO ES MOV. DIFERENCIA NORMAL    *00004800
004900*                            1 - MOV. APLICADO FUERA PERIODO     *00004900
005000*                            2 - ES UN MOV. DE CORRECION         *00005000
005100* NUN-INCIDEN                NUMERO DE INCIDENCIA                *00005100
005110* IND-FORMPAGO               INDICADOR FORMA DE PAGO             *00005110
005200*                            0 - CAJA                            *00005200
005300*                            1 - CUENTA                          *00005300
005400*                            2 - CHEQUE                          *00005400
005500* IMP-PAGO                   IMPORTE DEL PAGO                    *00005500
005510* COD-DIVI-PAGO              DIVISA DEL IMPORTE ANTERIOR         *00005510
005520* COD-ENTCHEQU               ENTIDAD DEL CHEQUE                  *00005520
005600* COD-OFICHEQU               OFICINA DEL CHEQUE                  *00005600
005700* COD-CTACHEQU               CUENTA DEL CHEQUE                   *00005700
005900* NUM-DOCCHEQU               NUMERO DE DOCUMENTO                 *00005900
006000* TIP-DOCCHEQU               TIPO DE DOCUMENTO                   *00006000
006100* FEC-DISPCHEQU              FECHA DEPOSITO CHEQUE               *00006100
006110* COD-PLAZA                  CODIGO DE PLAZA                     *00006110
006200* ENTIDAD-PAG                ENTIDAD                             *00006200
006300* CENTRO-PAG                 CENTRO                              *00006300
006400* CUENTA-PAG                 NUMERO DE CUENTA                    *00006400
006500* DIGICCC1-PAG               DIGITO 1 DE CONTROL                 *00006500
006600* DIGICCC2-PAG               DIGITO 2 DE CONTROL                 *00006600
006601* CBO-TASA                                                       *00006601
006602* TASA-ANT                   TASA ANTERIOR EN UN CAMBIO DE TASA  *00006602
006603* TASA-NUEVA                 TASA NUEVA EN UN CAMBIO DE TASA     *00006603
006610* RETROCESION.                                                   *00006610
006620* INDRETRO                   INDICADOR DE RETROCESION            *00006620
006630* FECRETRO                   FECHA DE RETROCESION                *00006630
006640* STAMP-RETRO.                                                   *00006640
006650* ENTIDAD-RETRO              ENTIDAD DE RETROCESION              *00006650
006660* CENTRO-RETRO               CENTRO DE RETROCESION               *00006660
006670* USERID-RETRO               USUARIO DE RETROCESION              *00006670
006680* NETNAME-RETRO              TERMINAL DE RETROCESION             *00006680
006690* TIMESTAMP-RETRO            TIMESTAMP DE RETROCESION            *00006690
006691* TABLA-MODIF                                                    *00006691
006692* PRE_TABLA                  TABLA A LA QUE PERTENECE EL CAMPO   *00006692
006693*                            MODIFICADO                          *00006693
006694* NOM_CAMPO                  NOMBRE DEL CAMPO                    *00006694
006695* VALOR_ANT                  VALOR ANTIGUO                       *00006695
006696* VALOR_NUE                  VALOR NUEVO                         *00006696
006697* COD_TRANSACCION            CÓDIGO DE EVENTO - TRANSACCION      *00006697
006700* ENTIDAD-UMO                ENTIDAD ULTIMA MODIFICACION         *00006700
006800* CENTRO-UMO                 CENTRO ULTIMA MODIFICACION          *00006800
006900* USERID-UMO                 USUARIO ULTIMA MODIFICACION         *00006900
007000* NETNAME-UMO                TERMINAL ULTIMA MODIFICACION        *00007000
007100* TIMESTAMP                  TIMESTAMP ULTIMA MODIFICACION       *00007100
007200*                                                                *00007200
007300******************************************************************00007300
007400 02  UGTCMOV.                                                     00007400
007500   05 MOV-CLAVE.                                                  00007500
007600      10 MOV-CUENTA              PIC X(12).                       00007600
007700      10 MOV-OFICINA             PIC X(4).                        00007700
007800      10 MOV-ENTIDAD             PIC X(4).                        00007800
007900      10 MOV-NIO                 PIC X(24).                       00007900
008000      10 MOV-CODCONLI            PIC X(3).                        00008000
008010      10 MOV-NUM-SECUENCIA       PIC S9(3)V USAGE COMP-3.         00008010
008100   05 MOV-DATOS.                                                  00008100
008200      10 MOV-COD-EVENTO          PIC X(4).                        00008200
008210      10 MOV-TIPO-MOV            PIC X(1).                        00008210
008220         88 MOV-88-TIPO-MOV-CONTABLE   VALUE 'C'.                 00008220
008230         88 MOV-88-TIPO-MOV-FACTURAC   VALUE 'F'.                 00008230
008240         88 MOV-88-TIPO-MOV-GENERAL    VALUE 'G'.                 00008240
008300      10 MOV-DATOS-CALENDAR.                                      00008300
008400         15 MOV-FEOPER           PIC X(10).                       00008400
008500         15 MOV-FECONTA          PIC X(10).                       00008500
008600         15 MOV-FEVALOR          PIC X(10).                       00008600
008700         15 MOV-FELIQ            PIC X(10).                       00008700
008800      10 MOV-DATOS-OPERACION.                                     00008800
008900         15 MOV-CLAOPER          PIC X(2).                        00008900
009000         15 MOV-ENTIOPE          PIC X(4).                        00009000
009100         15 MOV-OFIOPE           PIC X(4).                        00009100
009200      10 MOV-DATOS-IMPORTE.                                       00009200
009300         15 MOV-IMPMOVI          PIC S9(13)V9(4) USAGE COMP-3.    00009300
009400         15 MOV-COD-DIVISA       PIC X(3).                        00009400
009500         15 MOV-TIP-CAMBIO-OPE   PIC S9(6)V9(5) USAGE COMP-3.     00009500
009600      10 MOV-DATOS-VARIOS.                                        00009600
009700         15 MOV-SALREAL          PIC S9(13)V9(4) USAGE COMP-3.    00009700
009900         15 MOV-UGYINCOR         PIC X(1).                        00009900
010000         15 MOV-UGYINDIF         PIC X(1).                        00010000
010010         15 MOV-NUN-INCIDEN      PIC S9(9)V USAGE COMP-3.         00010010
010100      10 MOV-FORMAPAGO.                                           00010100
010200         15 MOV-IND-FORMPAGO       PIC X(1).                      00010200
010210         15 MOV-IMP-PAGO           PIC S9(13)V9(4) USAGE COMP-3.  00010210
010220         15 MOV-COD-DIVI-PAGO      PIC X(3).                      00010220
010300         15 MOV-DATOS-CHEQUE.                                     00010300
010400            20 MOV-NUM-CHEQUE.                                    00010400
010500               25 MOV-COD-ENTCHEQU PIC X(4).                      00010500
010600               25 MOV-COD-OFICHEQU PIC X(4).                      00010600
010700               25 MOV-COD-CTACHEQU PIC X(12).                     00010700
010900            20 MOV-NUM-DOCCHEQU   PIC S9(13) USAGE COMP-3.        00010900
011000            20 MOV-TIP-DOCCHEQU   PIC x(2).                       00011000
011100            20 MOV-FEC-DISPCHEQU  PIC X(10).                      00011100
011110            20 MOV-COD-PLAZA      PIC X(8).                       00011110
011200         15 MOV-CCC-PAG.                                          00011200
011300            20 MOV-ENTIDAD-PAG    PIC X(4).                       00011300
011400            20 MOV-CENTRO-PAG     PIC X(4).                       00011400
011500            20 MOV-CUENTA-PAG     PIC X(12).                      00011500
011600            20 MOV-DIGICCC1-PAG   PIC X(1).                       00011600
011700            20 MOV-DIGICCC2-PAG   PIC X(1).                       00011700
011701      10 MOV-CBO-TASA.                                            00011701
011702         15 MOV-TASA-ANT          PIC S9(03)V9(6) USAGE COMP-3.   00011702
011703         15 MOV-TASA-NUEVA        PIC S9(03)V9(6) USAGE COMP-3.   00011703
011710      10 MOV-RETROCESION.                                         00011710
011720         15 MOV-INDRETRO          PIC X(1).                       00011720
011730         15 MOV-FECRETRO          PIC X(10).                      00011730
011740         15 MOV-STAMP-RETRO.                                      00011740
011750            20 MOV-ENTIDAD-RETRO  PIC X(4).                       00011750
011760            20 MOV-CENTRO-RETRO   PIC X(4).                       00011760
011770            20 MOV-USERID-RETRO   PIC X(8).                       00011770
011780            20 MOV-NETNAME-RETRO  PIC X(8).                       00011780
011790            20 MOV-TIMESTAMP-RETRO  PIC X(26).                    00011790
011800      10 MOV-TABLA-MODIF.                                         00011800
011810         15 MOV-PRE-TABLA         PIC X(4).                       00011810
011830         15 MOV-NOM-CAMPO         PIC X(20).                      00011830
011840         15 MOV-VALOR-ANT         PIC X(20).                      00011840
011850         15 MOV-VALOR-NUE         PIC X(20).                      00011850
011851      10 MOV-COD-TRANSACCION      PIC X(4).                       00011851
011860      10 MOV-STAMP-UMO.                                           00011860
011900         15 MOV-ENTIDAD-UMO       PIC X(4).                       00011900
012000         15 MOV-CENTRO-UMO        PIC X(4).                       00012000
012100         15 MOV-USERID-UMO        PIC X(8).                       00012100
012200         15 MOV-NETNAME-UMO       PIC X(8).                       00012200
012300         15 MOV-TIMESTAMP         PIC X(26).                      00012300
