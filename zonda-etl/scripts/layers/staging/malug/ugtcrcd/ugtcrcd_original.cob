000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* ¡REA       - ACTIVO                                            *00000400
000500* APLICACI”N - PR…STAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCRCD                                    *00000700
000800* FECHA DE CREACI”N - 23-NOV-1999                                *00000800
000900*                                                                *00000900
001000* COPY DE LA TABLA DE DESGLOSE DE CONCEPTOS POR RECIBO           *00001000
001100*                                                                *00001100
001200******************************************************************00001200
001300*                     LOG DE MODIFICACIONES                      *00001300
001400* -------------------------------------------------------------- *00001400
001500* FECHA    | AUTOR     | DESCRIPCI”N                             *00001500
001600* -------------------------------------------------------------- *00001600
001700*                                                                *00001700
001800******************************************************************00001800
001900*                                                                *00001900
002000* CAMPO                      DESCRIPCIÛN                         *00002000
002100* -------------------------- ----------------------------------- *00002100
002200* CLAVE                                                          *00002200
002300*   CCC -------------------- CODIGO CUENTA CLIENTE               *00002300
002400*     CUENTA --------------- NUMERO DE CUENTA                    *00002400
002500*     OFICINA -------------- CODIGO DE OFICINA                   *00002500
002600*     ENTIDAD -------------- CODIGO DE ENTIDAD                   *00002600
002700*   FELIQ ------------------ FECHA DE VENCIMIENTO                *00002700
002800*   CODCONLI --------------- CODIGO DE CONCEPTO                  *00002800
002900*   COD-CONCPASO ----------- CODIGO DE CONCEPTO ASOCIADO AL CON_ *00002900
003000*                            CEPTO DESGLOSADO.                   *00003000
003100*   TIPOCPTO-ASOC ---------- TIPO DE CONCEPTO ASOCIADO           *00003100
003200* DATOS                                                          *00003200
003300*   TIPOCPTO --------------- TIPO DE CONCEPTO                    *00003300
003400*   COD-DIVISA ------------- CODIGO DE DIVISA                    *00003400
003500*   TIP-CAMBIO-LIQ --------- VALOR DE LA DIVISA A LA FECHA DE    *00003500
003600*                            LIQUIDACION DEL RECIBO.             *00003600
003610*   INT-ACEL-FAC -- ---------INT CALCULADOS DEV EN ACELERACION   *00003610
003620*   INT-ACEL-REC -- ---------INT RECUPERADO DEV EN ACELERACION   *00003620
003700*   IMPORTES.                                                    *00003700
003800*     IMP-FACTURAD --------- IMPORTE FACTURADO                   *00003800
003900*     IMP-RECUPERA --------- IMPORTE RECUPERADO                  *00003900
004000*   SEGUROS.                                                     *00004000
004100*     NUM-SEGURO ----------- NUMERO DE CONTRATO DE SEGURO        *00004100
004200*   IMPUESTOS.                                                   *00004200
004300*     POR-ALICUOTA --------- TASA APLICAR (ALICUOTA DEL IMPUESTO)*00004300
004400*     IND-LIQIMPUE --------- INDICADOR DE LIQUIDACION DEL IMPUES_*00004400
004500*                            TO                                  *00004500
004600*                            'D'-DEVENGO                         *00004600
004700*                            'P'-PERCIBIDO                       *00004700
004800*     IMP-BASE ------------- IMPORTE BASE DEL CALCULO DEL CONCEP_*00004800
004900*                            TO.                                 *00004900
005000* STAMP.                                                         *00005000
005100*   ENTIDAD-UMO  ------------- ENTIDAD ULTIMA MODIFICACION       *00005100
005200*   CENTRO-UMO   ------------- CENTRO ULTIMA MODIFICACION        *00005200
005300*   USERID-UMO   ------------- USUARIO ULTIMA MODIFICACION       *00005300
005400*   NETNAME-UMO  ------------- TERMINAL ULTIMA MODIFICACION      *00005400
005500*   TIMESTAMP    ------------- FECHA Y HORA ULTIMA MODIFICACION  *00005500
005600******************************************************************00005600
005700 02  UGTCRCD.                                                     00005700
005800   05 RCD-CLAVE.                                                  00005800
005900     10 RCD-CCC.                                                  00005900
006000       15 RCD-CUENTA           PIC X(12).                         00006000
006100       15 RCD-OFICINA          PIC X(4).                          00006100
006200       15 RCD-ENTIDAD          PIC X(4).                          00006200
006300     10 RCD-FELIQ            PIC X(10).                           00006300
006400     10 RCD-CODCONLI         PIC X(3).                            00006400
006500     10 RCD-COD-CONCPASO     PIC X(3).                            00006500
006600     10 RCD-TIPOCPTO-ASOC    PIC X(1).                            00006600
006700   05 RCD-DATOS.                                                  00006700
006800     10 RCD-TIPOCPTO         PIC X(1).                            00006800
006900     10 RCD-COD-DIVISA       PIC X(3).                            00006900
007000     10 RCD-TIP-CAMBIO-LIQ   PIC S9(6)V9(5) USAGE COMP-3.         00007000
007010     10 RCD-INT-ACEL-FAC     PIC S9(13)V9(4) USAGE COMP-3.        00007010
007020     10 RCD-INT-ACEL-REC     PIC S9(13)V9(4) USAGE COMP-3.        00007020
007100     10 RCD-IMPORTES.                                             00007100
007200       15 RCD-IMP-FACTURAD     PIC S9(13)V9(4) USAGE COMP-3.      00007200
007300       15 RCD-IMP-RECUPERA     PIC S9(13)V9(4) USAGE COMP-3.      00007300
007400     10 RCD-SEGUROS.                                              00007400
007500       15 RCD-NUM-SEGURO       PIC S9(9)V USAGE COMP-3.           00007500
007600     10 RCD-IMPUESTOS.                                            00007600
007700       15 RCD-POR-ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.       00007700
007800       15 RCD-IND-LIQIMPUE     PIC X(1).                          00007800
007900       15 RCD-IMP-BASE         PIC S9(13)V9(4) USAGE COMP-3.      00007900
008000   05 RCD-STAMP.                                                  00008000
008100     10 RCD-ENTIDAD-UMO      PIC X(4).                            00008100
008200     10 RCD-CENTRO-UMO       PIC X(4).                            00008200
008300     10 RCD-USERID-UMO       PIC X(8).                            00008300
008400     10 RCD-NETNAME-UMO      PIC X(8).                            00008400
008500     10 RCD-TIMESTAMP        PIC X(26).                           00008500
