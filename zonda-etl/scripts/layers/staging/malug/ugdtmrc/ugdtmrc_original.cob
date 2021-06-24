000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* ÁREA       - ACTIVO                                            *00000400
000500* APLICACIÓN - PRÉSTAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCMRC                                    *00000700
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
002500* FELIQ                      FECHA DE LIQUIDACION                *00002500
002600* NIO                        NUMERO INTERNO DE OPERACION         *00002600
002700* CODCONLI                   CONCEPTO DEL MOVIMIENTO             *00002700
002800* COD-CONCPASO               CODIGO CONCEPTO ASOCIADO AL         *00002800
002900*                            CONCEPTO DESGLOSADO                 *00002900
003000* TIPOCPTO-ASOC              TIPO DEL CONCEPTO ASOCIADO          *00003000
003100* TIPOCPTO                   TIPO DEL CONCEPTO                   *00003100
003200* COD-DIVISA                 MONEDA                              *00003200
003300* IMP-RECUPERA               IMPORTE RECUPERADO                  *00003300
003400* IMP-BASE                   IMPORTE BASE DEL CALCULO DEL        *00003400
003500*                            CONCEPTO IMPUESTO                   *00003500
003600* IMP-CAMBDIVL               VALOR DE LA DIVISA A FECHA DE LIQUI.*00003600
003700* IMP-CAMBDIVI               VALOR DE LA DIVISA A FECHA DE MOV.  *00003700
003800* POR-ALICUOTA               TASA APLICAR (ALICUOTA DEL IMPUESTO)*00003800
003900* IND-LIQIMPUE               INDICADOR LIQUIDACION DEL IMPUESTO  *00003900
004000*                            (D-DEVENGADO P-PERCIBIDO)           *00004000
004100* RETROCESION.                                                   *00004100
004200* INDRETRO                   INDICADOR DE RETROCESION            *00004200
004300* FECRETRO                   FECHA DE RETROCESION                *00004300
004400* STAMP-RETRO.                                                   *00004400
004500* ENTIDAD-RETRO              ENTIDAD DE RETROCESION              *00004500
004600* CENTRO-RETRO               CENTRO DE RETROCESION               *00004600
004700* USERID-RETRO               USUARIO DE RETROCESION              *00004700
004800* NETNAME-RETRO              TERMINAL DE RETROCESION             *00004800
004900* TIMESTAMP-RETRO            TIMESTAMP DE RETROCESION            *00004900
004910* FEOPER                     FECHA DE OPERACIÓN                  *00004910
004920* FECONTA                    FECHA CONTABLE                      *00004920
004930* FEVALOR                    FECHA VALOR                         *00004930
005000* ENTIDAD-UMO                ENTIDAD ULTIMA MODIFICACION         *00005000
005100* CENTRO-UMO                 CENTRO ULTIMA MODIFICACION          *00005100
005200* USERID-UMO                 USUARIO ULTIMA MODIFICACION         *00005200
005300* NETNAME-UMO                TERMINAL ULTIMA MODIFICACION        *00005300
005400* TIMESTAMP                  TIMESTAMP ULTIMA MODIFICACION       *00005400
005500*                                                                *00005500
005600******************************************************************00005600
005700 02  UGTCMRC.                                                     00005700
005800   05 MRC-CLAVE.                                                  00005800
005900      10 MRC-CUENTA           PIC X(12).                          00005900
006000      10 MRC-OFICINA          PIC X(4).                           00006000
006100      10 MRC-ENTIDAD          PIC X(4).                           00006100
006200      10 MRC-FELIQ            PIC X(10).                          00006200
006300      10 MRC-NIO              PIC X(24).                          00006300
006400      10 MRC-CODCONLI         PIC X(3).                           00006400
006500      10 MRC-COD-CONCPASO     PIC X(3).                           00006500
006600      10 MRC-TIPOCPTO-ASOC    PIC X(1).                           00006600
006700   05 MRC-DATOS.                                                  00006700
006800      10 MRC-TIPOCPTO         PIC X(1).                           00006800
006900      10 MRC-COD-DIVISA       PIC X(3).                           00006900
007000      10 MRC-IMP-RECUPERA     PIC S9(13)V9(4) USAGE COMP-3.       00007000
007100      10 MRC-IMP-BASE         PIC S9(13)V9(4) USAGE COMP-3.       00007100
007200      10 MRC-IMP-CAMBDIVL     PIC S9(6)V9(5) USAGE COMP-3.        00007200
007300      10 MRC-IMP-CAMBDIVI     PIC S9(6)V9(5) USAGE COMP-3.        00007300
007400   05 MRC-IMPUESTOS.                                              00007400
007500      10 MRC-POR-ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.        00007500
007600      10 MRC-IND-LIQIMPUE     PIC X(1).                           00007600
007700   05 MRC-RETROCESION.                                            00007700
007800      10 MRC-INDRETRO            PIC X(1).                        00007800
007900      10 MRC-FECRETRO            PIC X(10).                       00007900
008000      10 MRC-STAMP-RETRO.                                         00008000
008100         15 MRC-ENTIDAD-RETRO    PIC X(4).                        00008100
008200         15 MRC-CENTRO-RETRO     PIC X(4).                        00008200
008300         15 MRC-USERID-RETRO     PIC X(8).                        00008300
008400         15 MRC-NETNAME-RETRO    PIC X(8).                        00008400
008500         15 MRC-TIMESTAMP-RETRO  PIC X(26).                       00008500
008510   05 MRC-FEOPER              PIC X(10).                          00008510
008520   05 MRC-FECONTA             PIC X(10).                          00008520
008530   05 MRC-FEVALOR             PIC X(10).                          00008530
008600   05 MRC-STAMP-UMO.                                              00008600
008700      10 MRC-ENTIDAD-UMO      PIC X(4).                           00008700
008800      10 MRC-CENTRO-UMO       PIC X(4).                           00008800
008900      10 MRC-USERID-UMO       PIC X(8).                           00008900
009000      10 MRC-NETNAME-UMO      PIC X(8).                           00009000
009100      10 MRC-TIMESTAMP        PIC X(26).                          00009100
