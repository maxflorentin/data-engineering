000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* ÁREA       - ACTIVO                                            *00000400
000500* APLICACIÓN - PRÉSTAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCMVR                                    *00000700
000800* FECHA DE CREACIÓN - 24-nov-1999                                *00000800
000900*                                                                *00000900
001000* Descripción del registro                                       *00001000
001100* CONTIENE LOS MOVIMIENTOS DE COBRO DE UN RECIBO, UN REGISTRO    *00001100
001200* POR FECHA DE COBRO                                             *00001200
001300*                                                                *00001300
001400******************************************************************00001400
001500*                     LOG DE MODIFICACIONES                      *00001500
001600* -------------------------------------------------------------- *00001600
001700* FECHA    | AUTOR     | DESCRIPCIÓN                             *00001700
001800* -------------------------------------------------------------- *00001800
001900*                                                                *00001900
002000******************************************************************00002000
002100*                                                                *00002100
002200* CAMPO                      DESCRIPCIóN                         *00002200
002300* -------------------------- ----------------------------------- *00002300
002400*  CUENTA            CUENTA DEL PRESTAMO/AVAL                    *00002400
002500*  OFICINA           OFICINA DE LA CUENTA DEL PRESTAMO/AVAL      *00002500
002600*  ENTIDAD           ENTIDAD POSEEDORA DEL PRESTAMO/AVAL         *00002600
002700*  FELIQ             FECHA DE LIQUIDACIóN DEL RECIBO             *00002700
002800*  NIO               NUMERO INTERNO OPERACION(FECHA-HORA)        *00002800
002900*  NUMREC            NUMERO DE RECIBO DENTRO DEL PRESTAMO        *00002900
003000*  FEOPER            FECHA DE OPERACION DEL MOVIMIENTO           *00003000
003100*  FECONTA           FECHA CONTABLE DEL MOVIMIENTO               *00003100
003200*  FEVALOR           FECHA VALOR DEL MOVIMIENTO                  *00003200
003300*  CAPINIRE          IMPORTE DE CAPITAL DEL MOVIMIENTO           *00003300
003400*  IND-DESGCAPI      INDICADOR DE DESGLOSE DE CAPITAL            *00003400
003500*  CODCONLI-CAP      CONCEPTO DE LIQUIDACION DE CAPITAL          *00003500
003510*  IND-DESG-REAJCAP  CONCEPTO DE LIQUIDACION DE CAPITAL          *00003510
003520*  IND-DESG-REAJSEG  CONCEPTO DE LIQUIDACION DE CAPITAL          *00003520
003600*  INTINIRE          IMPORTE DE INTERESES DEL MOVIMIENTO         *00003600
003700*  IND-DESGINTE      INDICADOR DE DESGLOSE DE INTERESES          *00003700
003800*  CODCONLI-INT      CONCEPTO DE LIQUIDACION DE INTERESES        *00003800
003900*  IND-DESGCOMI      INDICADOR DE DESGLOSE DE COMISIONES         *00003900
004000*  COMINIRE          IMPORTE DE COMISONES DEL RECIBO             *00004000
004100*  CODCONLI-COM      CONCEPTO DE LIQUIDACIÓN DE COMISONES        *00004100
004200*  IND-DESGGAST      INDICADOR DE DESGLOSE DE GASTOS             *00004200
004300*  GASINIRE          IMPORTE DE GASTOS DEL RECIBO                *00004300
004400*  CODCONLI-GAS      CONCEPTO DE LIQUIDACION DE GASTOS           *00004400
004500*  IND-DESGSEGU      INDICADOR DE DESGLOSE DE SEGUROS            *00004500
004600*  SEGINIRE          IMPORTE DE SEGUROS DEL MOVIMIENTO           *00004600
004700*  CODCONLI-SEG      COMCEPTO DE LIQUIDACION DE SEGUROS          *00004700
004800*  IND-DESGIMPU      INDICADOR DE DESGLOSE DE IMPUESTOS          *00004800
004900*  IMPINIRE          IMPORTE DE IMPUESTOS DEL MOVIMIENTO         *00004900
005000*  CODCONLI-IMP      CONCEPTO DE LIQUIDACION DE IMPUESTOS        *00005000
005100*  POR-ALICUOTA      TASA A APLICAR DE IMPUESTOS                 *00005100
005200*  IND-LIQIMPUE      INDICADOR DE LIQUIDACION DE IMPUESTO        *00005200
005300*                    'D' - DEVENGO                               *00005300
005400*                    'P' - PERCIBIDO                             *00005400
005500*  IMP-BASE          IMPORTE BASE DEL CONCEPTO DEL IMPUESTO      *00005500
005600*  FECALMORA         FECHA DE INICIO DEL PERIODO DE MORA LIQUIDADO00005600
005700*  IND-DESGMORA      INDICADOR DE DESGLOSE DE MORA               *00005700
005800*  CODCONLI-MOR      CODIGO DEL CONCEPTO DE LIQUIDACION DE MORA  *00005800
005900*  IMP-MORA          IMPORTE DE INTERESES DE MORA DEL MOVIMIENTO *00005900
006000*  IMP-CONCOBEX      IMPORTE COMISION PARA EMPR.COBRANZA EXTERNA *00006000
006100*  CODCONLI-COBEX    CODIGO DE CONCEPTO DE LIQUIDACION DE        *00006100
006200*                    EMPRESAS DE COBRANZA EXTERNA                *00006200
006300*  IND-DESGCOBE      INDICADOR DE DESGLOSE DE COMISIONES DE      *00006300
006400*                    DE COBRANZA EXTERNA                         *00006400
006500*  IND-DESGCPS       INDICADOR DE DESGLOSE DE COMPENSATORIOS     *00006500
006600*  CODCONLI-CPS      CODIGO DE CONCEPTO DE LIQUIDACION DE        *00006600
006700*                    INTERES COMPENSATORIOS                      *00006700
006800*  IMP-CPS           IMPORTE DE INTERESES COMPENSATORIOS DEL     *00006800
006900*                    MOVIMIENTO                                  *00006900
007000*  SALREAL           SALDO REAL DE LA CUENTA PREVIO AL MVTO.     *00007000
007100*  IND-FORMPAGO      INDICADOR DE FORMA DE PAGO:                 *00007100
007200*                    '0' - CAJA                                  *00007200
007300*                    '1' - CUENTA                                *00007300
007400*                    '2' - CHEQUE                                *00007400
007500*  IMP-PAGO          IMPORTE DEL PAGO                            *00007500
007600*  COD-DIVI-PAGO     DIVISA DEL IMPORTE ANTERIOR                 *00007600
007700*  COD-ENTCHEQU      ENTIDAD EMISORA DEL CHEQUE                  *00007700
007800*  COD-OFICHEQU      OFICINA EMISORA DEL CHEQUE                  *00007800
007900*  COD-CTACHEQU      CUENTA EMISORA DEL CHEQUE                   *00007900
008000*  NUM-DOCCHEQU      NUMERO DE DOCUMENTO DEL CHEQUE              *00008000
008100*  TIP-DOCCHEQU      TIPO DE DOCUMENTO DEL CHEQUE                *00008100
008200*  FEC-DISPCHEQU     FECHA DE DEPOSITO DEL CHEQUE                *00008200
008300*  COD-PLAZA         CODIGO DE PLAZA                             *00008300
008400*  ENTIDAD-PAG       ENTIDAD DE LA CUENTA DE PASIVO QUE PAGO     *00008400
008500*                    EL RECIBO                                   *00008500
008600*  CENTRO-PAG        CENTRO DE LA CUENTA DE PASIVO               *00008600
008700*  CUENTA-PAG        CUENTA DE PASIVO QUE PAGO EL CHEQUE         *00008700
008800*  DIGICCC1-PAG      DIGITO CONTROL 1                            *00008800
008900*  DIGICCC2-PAG      DIGITO CONTROL 2                            *00008900
009000*  COD-DIVISA        DIVISA DEL MOVIMIENTO                       *00009000
009100*  IMP-CAMBDIVI      VALOR DE LA DIVISA A LA FECHA DE LA OPERACION00009100
009200*  SITDEUCT          SITUACION EN LA QUE SE COBRO EL RECIBO      *00009200
009300*  TIP-CONDONAR      TIPO DE CONDONACION                         *00009300
009400*  COD-EVENTO        CODIGO DE EVENTO                            *00009400
009500*  NUM-COB-CTSO      RECIBOS COBRADOS PARXIALMENTE EN CONTENCIOSO*00009500
009600*  RETROCESION.                                                  *00009600
009700*  INDRETRO          INDICADOR DE RETROCESION                    *00009700
009800*  FECRETRO          FECHA DE RETROCESION                        *00009800
009900*  STAMP-RETRO.                                                  *00009900
010000*  ENTIDAD-RETRO     ENTIDAD DE RETROCESION                      *00010000
010100*  CENTRO-RETRO      CENTRO DE RETROCESION                       *00010100
010200*  USERID-RETRO      USUARIO DE RETROCESION                      *00010200
010300*  NETNAME-RETRO     TERMINAL DE RETROCESION                     *00010300
010400*  TIMESTAMP-RETRO   TIMESTAMP DE RETROCESION                    *00010400
010500*  ENTIDAD-UMO       ENTIDAD ULTIMA MODIFICACION                 *00010500
010600*  CENTRO-UMO        CENTRO ULTIMA MODIFICACION                  *00010600
010700*  USERID-UMO        USUARIO ULTIMA MODIFICACION                 *00010700
010800*  NETNAME-UMO       TERMINAL ULTIMA MODIFICACION                *00010800
010900*  TIMESTAMP-UMO     TIMESTAMP ULTIMA MODIFICACION               *00010900
011000******************************************************************00011000
011100 02  UGTCMVR.                                                     00011100
011200     05 MVR-CLAVE.                                                00011200
011300        10 MVR-CUENTA              PIC X(12).                     00011300
011400        10 MVR-OFICINA             PIC X(4).                      00011400
011500        10 MVR-ENTIDAD             PIC X(4).                      00011500
011600        10 MVR-FELIQ               PIC X(10).                     00011600
011700        10 MVR-NIO                 PIC X(24).                     00011700
011800     05 MVR-DATOS.                                                00011800
011900        10 MVR-NUMREC              PIC S9(5)V USAGE COMP-3.       00011900
012000        10 MVR-FECHAS-MVTO-COBRO.                                 00012000
012100           15 MVR-FEOPER           PIC X(10).                     00012100
012200           15 MVR-FECONTA          PIC X(10).                     00012200
012300           15 MVR-FEVALOR          PIC X(10).                     00012300
012400        10 MVR-CAPITAL.                                           00012400
012500           15 MVR-CAPINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00012500
012600           15 MVR-IND-DESGCAPI     PIC X(1).                      00012600
012700           15 MVR-CODCONLI-CAP     PIC X(3).                      00012700
012710           15 MVR-IND-DESG-REAJCAP PIC X(1).                      00012710
012720           15 MVR-IND-DESG-REAJSEG PIC X(1).                      00012720
012800        10 MVR-INTERESES.                                         00012800
012900           15 MVR-INTINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00012900
013000           15 MVR-IND-DESGINTE     PIC X(1).                      00013000
013100           15 MVR-CODCONLI-INT     PIC X(3).                      00013100
013200        10 MVR-COMISIONES.                                        00013200
013300           15 MVR-IND-DESGCOMI     PIC X(1).                      00013300
013400           15 MVR-COMINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00013400
013500           15 MVR-CODCONLI-COM     PIC X(3).                      00013500
013600        10 MVR-GASTOS.                                            00013600
013700           15 MVR-IND-DESGGAST     PIC X(1).                      00013700
013800           15 MVR-GASINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00013800
013900           15 MVR-CODCONLI-GAS     PIC X(3).                      00013900
014000        10 MVR-SEGUROS.                                           00014000
014100           15 MVR-IND-DESGSEGU     PIC X(1).                      00014100
014200           15 MVR-SEGINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00014200
014300           15 MVR-CODCONLI-SEG     PIC X(3).                      00014300
014400        10 MVR-IMPUESTOS.                                         00014400
014500           15 MVR-IND-DESGIMPU     PIC X(1).                      00014500
014600           15 MVR-IMPINIRE         PIC S9(13)V9(4) USAGE COMP-3.  00014600
014700           15 MVR-CODCONLI-IMP     PIC X(3).                      00014700
014800           15 MVR-POR-ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.   00014800
014900           15 MVR-IND-LIQIMPUE     PIC X(1).                      00014900
015000              88 MVR-88-IND-LIQIMPUE-DEV      VALUE 'D'.          00015000
015100              88 MVR-88-IND-LIQIMPUE-PER      VALUE 'P'.          00015100
015200           15 MVR-IMP-BASE         PIC S9(13)V9(4) USAGE COMP-3.  00015200
015300        10 MVR-MORA.                                              00015300
015400           15 MVR-FECALMORA        PIC X(10).                     00015400
015500           15 MVR-IND-DESGMORA     PIC X(1).                      00015500
015600           15 MVR-CODCONLI-MOR     PIC X(3).                      00015600
015700           15 MVR-IMP-MORA         PIC S9(13)V9(4) USAGE COMP-3.  00015700
015800        10 MVR-COBRANZA-EXERNA.                                   00015800
015900           15 MVR-IMP-CONCOBEX     PIC S9(13)V9(4) USAGE COMP-3.  00015900
016000           15 MVR-CODCONLI-COBEX   PIC X(3).                      00016000
016100           15 MVR-IND-DESGCOBE     PIC X(1).                      00016100
016200        10 MVR-COMPENSATORIOS.                                    00016200
016300           15 MVR-IND-DESGCPS      PIC X(1).                      00016300
016400           15 MVR-CODCONLI-CPS     PIC X(3).                      00016400
016500           15 MVR-IMP-CPS          PIC S9(13)V9(4) USAGE COMP-3.  00016500
016600        10 MVR-SALREAL             PIC S9(13)V9(4) USAGE COMP-3.  00016600
016700        10 MVR-FORMAPAGO.                                         00016700
016800           15 MVR-IND-FORMPAGO     PIC X(1).                      00016800
016900              88 MVR-88-IND-FORMPAGO-CAJ      VALUE '0'.          00016900
017000              88 MVR-88-IND-FORMPAGO-CTA      VALUE '1'.          00017000
017100              88 MVR-88-IND-FORMPAGO-CHE      VALUE '2'.          00017100
017200           15 MVR-IMP-PAGO         PIC S9(13)V9(4) USAGE COMP-3.  00017200
017300           15 MVR-COD-DIVI-PAGO    PIC X(3).                      00017300
017400        10 NUM-CHEQUE.                                            00017400
017500           15 MVR-COD-ENTCHEQU     PIC X(4).                      00017500
017600           15 MVR-COD-OFICHEQU     PIC X(4).                      00017600
017700           15 MVR-COD-CTACHEQU     PIC X(12).                     00017700
017800           15 MVR-NUM-DOCCHEQU     PIC S9(13)V USAGE COMP-3.      00017800
017900           15 MVR-TIP-DOCCHEQU     PIC x(2).                      00017900
018000           15 MVR-FEC-DISPCHEQU    PIC X(10).                     00018000
018100           15 MVR-COD-PLAZA        PIC X(8).                      00018100
018200        10 MVR-CCC-PAG.                                           00018200
018300           15 MVR-ENTIDAD-PAG      PIC X(4).                      00018300
018400           15 MVR-CENTRO-PAG       PIC X(4).                      00018400
018500           15 MVR-CUENTA-PAG       PIC X(12).                     00018500
018600           15 MVR-DIGICCC1-PAG     PIC X(1).                      00018600
018700           15 MVR-DIGICCC2-PAG     PIC X(1).                      00018700
018800        10 MVR-COD-DIVISA          PIC X(3).                      00018800
018900        10 MVR-IMP-CAMBDIVI        PIC S9(6)V9(5) USAGE COMP-3.   00018900
019000        10 MVR-SITDEUCT            PIC X(2).                      00019000
019100        10 MVR-TIP-CONDONAR        PIC X(5).                      00019100
019200        10 MVR-COD-EVENTO          PIC X(4).                      00019200
019300        10 MVR-NUM-COB-CTSO        PIC S9(3)V USAGE COMP-3.       00019300
019400     05 MVR-RETROCESION.                                          00019400
019500        10 MVR-INDRETRO            PIC X(1).                      00019500
019600        10 MVR-FECRETRO            PIC X(10).                     00019600
019700        10 MVR-STAMP-RETRO.                                       00019700
019800           15 MVR-ENTIDAD-RETRO    PIC X(4).                      00019800
019900           15 MVR-CENTRO-RETRO     PIC X(4).                      00019900
020000           15 MVR-USERID-RETRO     PIC X(8).                      00020000
020100           15 MVR-NETNAME-RETRO    PIC X(8).                      00020100
020200           15 MVR-TIMESTAMP-RETRO  PIC X(26).                     00020200
020300     05 MVR-STAMP-UMO.                                            00020300
020400           15 MVR-ENTIDAD-UMO      PIC X(4).                      00020400
020500           15 MVR-CENTRO-UMO       PIC X(4).                      00020500
020600           15 MVR-USERID-UMO       PIC X(8).                      00020600
020700           15 MVR-NETNAME-UMO      PIC X(8).                      00020700
020800           15 MVR-TIMESTAMP-UMO    PIC X(26).                     00020800
020900******************************************************************00020900
021000* THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 64      *00021000
021100******************************************************************00021100
