000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* AREA       - ACTIVO                                            *00000400
000500* APLICACION - PRESTAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCREC                                    *00000700
000800* FECHA DE CREACION - 23-NOV-1999                                *00000800
000900*                                                                *00000900
001000* Descripcion del registro                                       *00001000
001100* COPY DE LA TABLA DE RECIBOS DE PRESTAMOS                       *00001100
001200******************************************************************00001200
001300*                     LOG DE MODIFICACIONES                      *00001300
001600*                                                                *00001400
000061******************************************************************00001500
000062* MODIFICACION : ALTEC-0003                                      *00001600
000063* PETICION     : 08-04153-1A                                     *00001700
000064* AUTOR        : ALTEC - (SOLUSERVICIOS)  HFIGUEROA              *00001800
000065* FECHA        : 24-11-2008                                      *00001900
000066* DESCRIPCION  : se modifica descripcion de importe provisionado *00002000
000066*                por   importe rendicion a compania de seguros   *00002100
000800******************************************************************00002200
000061******************************************************************00002210
000062* MODIFICACION : ISBAN-0001                                      *00002220
000063* PETICION     : ID2864 - CALCULO CFT                            *00002230
000064* AUTOR        : ISBAN                                           *00002240
000065* FECHA        : 16-12-2014                                      *00002250
000066* DESCRIPCION  : SE AGREGAN CAMPOS CFTNA Y CFTNA-SIMP            *00002260
000800******************************************************************00002280
001700*                                                                *00002300
001800******************************************************************00002400
001900*                                                                *00002500
002000* CAMPO                      DESCRIPCIÛN                         *00002600
002100* -------------------------- ----------------------------------- *00002700
002200* CLAVE.                                                         *00002800
002300*   CCC -------------------- CODIGO CUENTA CLIENTE               *00002900
002400*     CUENTA --------------- NUMERO DE CUENTA                    *00003000
002500*     OFICINA -------------- CODIGO DE OFICINA                   *00003100
002600*     ENTIDAD -------------- CODIGO DE ENTIDAD                   *00003200
002700*   FELIQ ------------------ FECHA DE LIQUIDACION DEL RECIBO     *00003300
002800* DATOS.                                                         *00003400
002900*   NUMREC ----------------- NUMERO DE RECIBO                    *00003500
003000*   INDPECO ---------------- INDICADOR DE ESTADO DEL RECIBO      *00003600
003100*                            '0' COBRADO EN SU TOTALIDAD         *00003700
003200*                            '1' PENDIENTE DE COBRO PARC./TOTAL  *00003800
003300*                            '2' PENDIENTE PRIMER INTENTO COBRO  *00003900
003400*                            '3' IMPAGO VOLUNTARIO               *00004000
003500*   UGYINCOR --------------- INDICADOR DE MOVIMIENTOS DE         *00004100
003600*                            CORRECION                           *00004200
003700*                            '0' NO EXISTEN MOVIMIENTOS          *00004300
003800*                            '1' EXISTEN MOVIMIENTOS             *00004400
003900*   FEC-ESPEPAGO ----------- FECHA ESPERADA DE PAGO              *00004500
004000*   FEC-EMIRECIB ----------- FECHA DE EMISION DEL RECIBO         *00004600
004100*   SALTEOR ---------------- SALDO TEORICO DEL CONTRATO          *00004700
004200*   FEINILIQ --------------- FECHA INICIO LIQUIDACION            *00004800
004300*   TAE -------------------- TASA ANUAL EFECTIVA                 *00004900
004400*   SITDEUOB --------------- SITUACION OBJETIVA DE DEUDA         *00005000
004500*   TIP-RECIBO ------------- TIPO DE RECIBO                      *00005100
004600*                            'D' DISPOSICION                     *00005200
004700*                            'L' LINEA                           *00005300
004800*                            'N' RECIBO NORMAL                   *00005400
004900*   IND-FAC-ONLINE---------- INDICADOR SI SE HIZO ONLINE O NO    *00005500
005000*   INT-ACEL-FAC -- ---------INT CALCULADOS DEV EN ACELERACION   *00005600
005100*   INT-ACEL-REC -- ---------INT RECUPERADO DEV EN ACELERACION   *00005700
005200*   CICLO-COMUNIC.                                               *00005800
005300*     CODCICLO ------------- CODIGO CICLO DE COMUNICACION        *00005900
005400*     FEPROCOM ------------- FECHA PROXIMA COMUNICACION          *00006000
005500*   CAPITAL.                                                     *00006100
005600*     IND-DESGCAPI --------- INDICADOR DE DESGLOSE DE CAPITAL    *00006200
005700*     CAPINIRE ------------- CAPITAL INICIAL FACTURADO EN RECIBO *00006300
005800*     CAPRECRE ------------- CAPITAL RECUPERADO                  *00006400
005900*     CODCONLI-CAP --------- CODIGO CONCEPTO DE LIQUIDACION DE   *00006500
006000*                            CAPITAL                             *00006600
006100*   INTERESES.                                                   *00006700
006200*     IND-DESGINTE --------- INDICADOR DE DESGLOSE DE INTERESES  *00006800
006300*     INTINIRE ------------- INTERESES INICIALES FACTURADOS EN   *00006900
006400*                            RECIBO                              *00007000
006500*     INTRECRE ------------- INTERESES RECUPERADOS               *00007100
006600*     CODCONLI-INT --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00007200
006700*                            DE INTERESES                        *00007300
006800*   COMISIONES.                                                  *00007400
006900*     IND-DESGCOMI --------- INDICADOR DE DESGLOSE DE COMISIONES *00007500
007000*     COMINIRE ------------- COMISIONES INICIALES FACTURADOS EN  *00007600
007100*                            RECIBO                              *00007700
007200*     COMRECRE ------------- COMISIONES RECUPERADAS              *00007800
007300*     CODCONLI-COM --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00007900
007400*                            DE COMISIONES                       *00008000
007500*   GASTOS.                                                      *00008100
007600*     IND-DESGGAST --------- INDICADOR DE DESGLOSE DE GATOS      *00008200
007700*     GASINIRE ------------- GASTOS     INICIALES FACTURADOS EN  *00008300
007800*                            RECIBO                              *00008400
007900*     GASRECRE ------------- GASTOS     RECUPERADOS              *00008500
008000*     CODCONLI-GAS --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00008600
008100*                            DE GASTOS                           *00008700
008200*   SUBVENCION.                                                  *00008800
008300*     IMPSUBVE ------------- IMPORTE SUBVENCIONADO               *00008900
008400*     CODCONLI-SUB --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00009000
008500*                            DE SUBVENCIONES                     *00009100
008600*   SEGUROS.                                                     *00009200
008700*     IND-DESGSEGU --------- INDICADOR DE DESGLOSE DE SEGUROS    *00009300
008800*     SEGINIRE ------------- SEGUROS    INICIALES FACTURADOS EN  *00009400
008900*                            RECIBO                              *00009500
009000*     SEGRECRE ------------- SEGUROS    RECUPERADOS              *00009600
009100*     NUM-SEGURO ----------- NUMERO DE CONTRATO DE SEGURO        *00009700
009200*     CODCONLI-SEG --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00009800
009300*                            DE SEGUROS                          *00009900
009400*   IMPUESTOS.                                                   *00010000
009500*     IND-DESGIMPU --------- INDICADOR DE DESGLOSE DE IMPUESTOS  *00010100
009600*     IMPINIRE ------------- IMPUESTOS  INICIALES FACTURADOS EN  *00010200
009700*                            RECIBO                              *00010300
009800*     IMPRECRE ------------- SEGUROS    RECUPERADOS              *00010400
009900*     IMP-BASE ------------- IMPORTE BASE DEL CALCULO DEL CONCEP_*00010500
010000*                            TO                                  *00010600
010100*     POR-ALICUOTA --------- TASA A APLICAR (ALICUOTA DEL IMPUES_*00010700
010200*                            TO)                                 *00010800
010300*     COD-CONCEIMP --------- CONCEPTO SOBRE EL QUE SE CALCULA EL *00010900
010400*                            IMPUESTO                            *00011000
010500*     IND-LIQIMPUE --------- INDICADOR DE LIQUIDACION DEL IMPUES_*00011100
010600*                            TO (D-DEVENGO;P-PERCIBIDO)          *00011200
010700*     CODCONLI-IMP --------- CODIGO DE CONCEPTO DE LIQUIDACION   *00011300
010800*                            DE IMPUESTOS                        *00011400
010900*   PROVISIONES.                                                 *00011500
010900*ALTEC-0003-I                                                    *00011600
011000******IMPROVI   ----------   IMPORTE PROVISIONADO DEL RECIBO     *00011700
011000*     IMPROVI -------------- IMPORTE A RENDIR A LA COMPANIA      *00011800
011400*                            DE SEGURO                           *00011900
010900*ALTEC-0003-I                                                    *00012000
011100*     FEULPROV ------------- FECHA DE ULTIMA PROVISION           *00012100
011200*   RETENCIONES.                                                 *00012200
011300*     FERETEN -------------- FECHA CONTABLE DE ULTIMA RETENCION  *00012300
011400*                            REALIZADA                           *00012400
011500*     SECURET -------------- SECUENCIA DE      ULTIMA RETENCION  *00012500
011600*                            REALIZADA                           *00012600
011700*     UGIRETEN ------------- IMPORTE RETENCION                   *00012700
011800*   UGNPLPEN   -------------  NUMERO DE RECIBOS PENDIENTES       *00012800
011900*   COBR-EXTERNA.                                                *00012900
012000*     COD-EMPCOBEX --------- CODIGO DE EMPRESA DE COBRANZA EXT.  *00013000
012100*     FEC-ENVCOBEX --------- FECHA ENVIO A COBRANZA EXTERNA      *00013100
012200*     CODCONLI-COBEX ------- CODIGO DE CONCEPTO DE LIQUIDACION   *00013200
012300*                            DE COBRANZA EXTERNA                 *00013300
012400*     IMP-COMCOBEXINI ------ IMPORTE DE COMISIONES PARA EMPRESAS *00013400
012500*                            DE COBRANZA EXTERNA CALCULADO       *00013500
012600*     IMP-COMCOBEXREC ------ IMPORTE DE COMISIONES PARA EMPRESAS *00013600
012700*                            DE COBRANZA EXTERNA RECUPERADO      *00013700
012800*     FEC-BAJCOBEX --------- FECHA DE BAJA EN LA EMPRESA DE      *00013800
012900*                            COBRANZA EXTERNA                    *00013900
013000*     IND-DESGCOBE --------- INDICADOR DE DESGLOSE DE COMISIONES *00014000
013100*                            DE COBRANZA EXTERNA.                *00014100
013200*   MORA.                                                        *00014200
013300*     CODCONLI-MOR   ------- CODIGO DE CONCEPTO DE LIQUIDACION   *00014300
013400*                            DE MORA.                            *00014400
013500*     IMP-MORCAL ----------- IMPORTE DE INTERESES MORATORIOS     *00014500
013600*                            CALCULADOS                          *00014600
013700*     IMP-MORREC ----------- IMPORTE DE INTERESES MORATORIOS     *00014700
013800*                            RECUPERADOS                         *00014800
013900*     IND-DESGMOR ---------- INDICADOR DE DESGLOSE DE INTERESES  *00014900
014000*                            MORATORIOS                          *00015000
014100*     TAS-MOR -------------- TASA A APLICAR EN INTERESES         *00015100
014200*                            MORATORIOS                          *00015200
014300*     IND_CAMB_TASA_MOR----- INDICADOR DE QUE SE HA VARIADO LA   *00015300
014400*                            TASA PARA EL CALCULO DE LOS INTERE- *00015400
014500*                            SES DE MORA EN EL PERIODO CALCULADO *00015500
014600*   COMPENSATOR.                                                 *00015600
014700*     CODCONLI-CPS   ------- CODIGO DE CONCEPTO DE LIQUIDACION   *00015700
014800*                            DE INTERESES COMPENSATORIOS         *00015800
014900*     IMP-CPSCAL ----------- IMPORTE DE INTERESES COMPENSATORIOS *00015900
015000*                            CALCULADOS                          *00016000
015100*     IMP-CPSREC ----------- IMPORTE DE INTERESES COMPENSATORIOS *00016100
015200*                            RECUPERADOS                         *00016200
015300*     IND-DESGCPS ---------- INDICADOR DE DESGLOSE DE INTERESES  *00016300
015400*                            COMPENSATORIOS                      *00016400
015500*     TAS-CPS -------------- TASA A APLICAR EN INTERESES         *00016500
015600*                            COMPENSATORIOS                      *00016600
015700*     FEC-CALCPS ----------- FECHA DE ULTIMO CALCULO DE          *00016700
015800*                            COMPENSATORIOS                      *00016800
015900*     IND_CAMB_TASA_CPS----- INDICADOR DE QUE SE HA VARIADO LA   *00016900
016000*                            TASA PARA EL CALCULO DE LOS INTERE- *00017000
016100*                            SES COMPENSATORIOS                  *00017100
016200*   DEUDA.                                                       *00017200
016300*     SITDEUCT ------------- SITUACION DEUDA CONTABLE            *00017300
016400*     FEC-SITCONT ---------- FECHA DE SITUACION CONTABLE DE LA   *00017400
016500*                            DEUDA                               *00017500
016600*   TIPOTIT ---------------- TIPO INTERES TITULAR                *00017600
016700*   TIPOTOT ---------------- TIPO INTERES TOTAL                  *00017700
016800*   UGYESCON --------------- INDICADOR DE RECIBO CONTABILIZADO   *00017800
016900*                            '0'-RECIBO NO CONTABILIZADO         *00017900
017000*                            '1'-INTERESES EN PRODUCTOS          *00018000
017100*                            '2'-INTERESES EN CUENTAS DE ORDEN   *00018100
017200*                            '3'-                                *00018200
017300*   UGYCVINT --------------- INDICADOR DE CAMBIOS DE TIPOS DE    *00018300
017400*                            INTERES                             *00018400
017500*                            '0'-SIN CAMBIO                      *00018500
017600*                            '1'-SE HAN PRODUCIDO CAMBIOS DEL    *00018600
017700*                            TIPO DE INTERES                     *00018700
017800*   FECALMOR --------------- FECHA ULTIMO CALCULO DE MORA        *00018800
017900*   FEC-HASTAMOR ----------- FECHA HASTA DEL CALCULO DE MORA     *00018900
018000*   IND-MORACOND ----------- INDICADOR DE MORA CONDONADA POR     *00019000
018100*                            CONVENIO DE EMPRESA                 *00019100
018200*   CAPITALIZ                                                     00019200
018300*   IMP-CAPITALIZ-----------                                      00019300
018400*   DISPONER EN EL RECIBO DE LA PARTE DE INTERESES CAPITALIZADOS  00019400
018500*   CODCONLI-CPZ------------                                      00019500
018600*   CÛDIGO DE CONCEPTO ESPECÌFICO PARA INTERESES CAPITALIZADOS.   00019600
018700*   DIFERIDOS                                                     00019700
018800*   IMP-DIFERIDO---------------                                   00019800
018900*   DISPONER EN EL RECIBO DEL IMPORTE DE INTERESES DIFERIDOS FACTU00019900
019000*   CODCONLI-DIF---------------                                   00020000
019100*   CÛDIGO DE CONCEPTO ESPECÌFICO PARA INTERESES DIFERIDOS FACTUR 00020100
019200*   BALLOOM                                                       00020200
019300*   IMP_BALLOOM---------------                                    00020300
019400*   IMPORTE DEL BALLOOM GENERADO EN EL PERIODO                    00020400
019500*   CODCONLI-BAL--------------                                    00020500
019600*   CÛDIGO DE CONCEPTO ESPECÌFICO PARA CUOTA BALLOOM.             00020600
019700*   SDO_BALLOOM --------------                                    00020700
019800*   SALDO CORRESPONDIENTE AL BALLOPM DEL VENCIMIENTO.             00020800
019900*   BALLOOM_FAC              IMPORTE DEL BALLOOM FACTURADO EN EL  00020900
020000*                            periodo                              00021000
020100*   TIP-CAMB-FELIQ.......... TIPO DE CAMBIO A FECHA DE VTO REC    00021100
020200*   IND-PAGO-ANTIC.......... INDICADOR DE PAGO ANTICIPADO         00021200
020300*   INT-ORDEN............... INTERESES EN CUENTAS DE ORDEN        00021300
020400*   REA-ORDEN............... REAJUSTES EN CUENTAS DE ORDEN        00021400
020500*   EJERCICIO-CAST.......... EJERCICIO EN EL QUE SE PRODUCE EL CAS00021500
020600*   IND_SEGCESAN............ INDICADOR DE SEGURO DE CESANTÌA     *00021600
020700*                    ***MODIFICACIONES IMPLANTACION              *00021700
020800*   TIP-TMC-APL              CRITERIO APLICADO C·LCULO MORATORIOS*00021800
020900*   REC-IND-LIQ-COBEX        INDICADOR LIQUIDACION COBR. EXTERNA *00021900
021000*   IMP-CAST-CUOTA           IMPORTE CASTIGADO DE LA CUOTA       *00022000
021100*   IND-CAST-TOT             IND.DE CUOTA CASTIGADA EN SU TOTAL  *00022100
021200*                                                                *00022200
020700*                    ***MODIFICACIONES INDEXACION                *00022300
      *   COEFICI-INDEX   ------- COEFICIENTE DE INDEXACIÛN            *00022400
      *   FACTOR-INDEX    ------- FACTOR DE INDEXACIÛN A APLICAR       *00022500
      *   IMPAJUST-SAL    ------- IMPORTE DE AJUSTE AL SALDO           *00022600
      *   CODCONLI-AJUSAL ------- CODCONLI DE AJUSTE DE SALDO          *00022700
      *   IMPAJUST-CAP    ------- IMPORTE DE AJUSTE DE CAPITAL         *00022800
      *   CODCONLI-AJUCAP ------- CODCONLI DE AJUSTE DE CAPITAL        *00022900
      *   IMPAJUST-INT    ------- IMPORTE DE AJUSTE DE INTERÈS         *00023000
      *   CODCONLI-AJUINT ------- CODCONLI DE AJUSTE DE INTERÈS        *00023100
021800*                                                                *00023200
021900* STAMP.                                                         *00023300
022000*   ENTIDAD-UMO ----------- ENTIDAD ULTIMA MODIFICACION          *00023400
022100*   CENTRO-UMO  ----------- CENTRO ULTIMA MODIFICACION           *00023500
022200*   USERID-UMO  ----------- USUARIO ULTIMA MODIFICACION          *00023600
022300*   NETNAME-UMO ----------- TERMINAL ULTIMA MODIFICACION         *00023700
022400*   TIMESTAMP   ----------- FECHA Y HORA ULTIMA MODIFICACION     *00023800
022500******************************************************************00023900
022600 02  UGTCREC.                                                     00024000
022700   05 REC-CLAVE.                                                  00024100
022800     10 REC-CCC.                                                  00024200
022900       15 REC-CUENTA           PIC X(12).                         00024300
023000       15 REC-OFICINA          PIC X(4).                          00024400
023100       15 REC-ENTIDAD          PIC X(4).                          00024500
023200     10 REC-FELIQ            PIC X(10).                           00024600
023300   05 REC-DATOS.                                                  00024700
023400     10 REC-NUMREC           PIC S9(5)V USAGE COMP-3.             00024800
023500     10 REC-INDPECO          PIC X(1).                            00024900
023600       88 REC-88-VAL-INDPECO          VALUE '0', '1', '2', '3'.   00025000
023700       88 REC-88-INDPECO-CO-TOT       VALUE '0'.                  00025100
023800       88 REC-88-INDPECO-PTE-CO       VALUE '1'.                  00025200
023900       88 REC-88-INDPECO-PTE-PR       VALUE '2'.                  00025300
024000       88 REC-88-INDPECO-IMP-VOL      VALUE '3'.                  00025400
024100     10 REC-UGYINCOR         PIC X(1).                            00025500
024200       88 REC-88-VAL-UGYINCOR         VALUE '0', '1'.             00025600
024300       88 REC-88-UGYINCOR-NOMOV       VALUE '0'.                  00025700
024400       88 REC-88-UGYINCOR-MOV         VALUE '1'.                  00025800
024500     10 REC-FEC-ESPEPAGO     PIC X(10).                           00025900
024600     10 REC-FEC-EMIRECIB     PIC X(10).                           00026000
024700     10 REC-SALTEOR          PIC S9(13)V9(4) USAGE COMP-3.        00026100
024800     10 REC-FEINILIQ         PIC X(10).                           00026200
024900     10 REC-TAE              PIC S9(3)V9(6) USAGE COMP-3.         00026300
025000     10 REC-SITDEUOB         PIC X(2).                            00026400
025100     10 REC-TIP-RECIBO       PIC X(1).                            00026500
025200       88 REC-88-VAL-TIP-RECIBO       VALUE 'D', 'L', 'N'.        00026600
025300       88 REC-88-TIP-RECIBO-D         VALUE 'D'.                  00026700
025400       88 REC-88-TIP-RECIBO-L         VALUE 'L'.                  00026800
025500       88 REC-88-TIP-RECIBO-N         VALUE 'N'.                  00026900
025600     10 REC-IND-FAC-ONLINE     PIC X(1).                          00027000
025700     10 REC-INT-ACEL-FAC       PIC S9(13)V9(4) COMP-3.            00027100
025800     10 REC-INT-ACEL-REC       PIC S9(13)V9(4) COMP-3.            00027200
025900     10 REC-CICLO-COMUNIC.                                        00027300
026000       15 REC-CODCICLO         PIC X(2).                          00027400
026100       15 REC-FEPROCOM         PIC X(10).                         00027500
026200     10 REC-CAPITAL.                                              00027600
026300       15 REC-IND-DESGCAPI     PIC X(1).                          00027700
026400       15 REC-CAPINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00027800
026500       15 REC-CAPRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00027900
026600       15 REC-CODCONLI-CAP     PIC X(3).                          00028000
026700     10 REC-INTERESES.                                            00028100
026800       15 REC-IND-DESGINTE     PIC X(1).                          00028200
026900       15 REC-INTINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00028300
027000       15 REC-INTRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00028400
027100       15 REC-CODCONLI-INT     PIC X(3).                          00028500
027200     10 REC-COMISIONES.                                           00028600
027300       15 REC-IND-DESGCOMI     PIC X(1).                          00028700
027400       15 REC-COMINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00028800
027500       15 REC-COMRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00028900
027600       15 REC-CODCONLI-COM     PIC X(3).                          00029000
027700     10 REC-GASTOS.                                               00029100
027800       15 REC-IND-DESGGAST     PIC X(1).                          00029200
027900       15 REC-GASINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00029300
028000       15 REC-GASRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00029400
028100       15 REC-CODCONLI-GAS     PIC X(3).                          00029500
028200     10 REC-SUBVENCION.                                           00029600
028300       15 REC-IMPSUBVE         PIC S9(13)V9(4) USAGE COMP-3.      00029700
028400       15 REC-CODCONLI-SUB     PIC X(3).                          00029800
028500     10 REC-SEGUROS.                                              00029900
028600       15 REC-IND-DESGSEGU     PIC X(1).                          00030000
028700       15 REC-SEGINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00030100
028800       15 REC-SEGRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00030200
028900       15 REC-NUM-SEGURO       PIC S9(9)V USAGE COMP-3.           00030300
029000       15 REC-CODCONLI-SEG     PIC X(3).                          00030400
029100     10 REC-IMPUESTOS.                                            00030500
029200       15 REC-IND-DESGIMPU     PIC X(1).                          00030600
029300       15 REC-IMPINIRE         PIC S9(13)V9(4) USAGE COMP-3.      00030700
029400       15 REC-IMPRECRE         PIC S9(13)V9(4) USAGE COMP-3.      00030800
029500       15 REC-IMP-BASE         PIC S9(13)V9(4) USAGE COMP-3.      00030900
029600       15 REC-POR-ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.       00031000
029700       15 REC-COD-CONCEIMP     PIC X(3).                          00031100
029800       15 REC-IND-LIQIMPUE     PIC X(1).                          00031200
029900       15 REC-CODCONLI-IMP     PIC X(3).                          00031300
030000     10 REC-PROVISIONES.                                          00031400
030100       15 REC-IMPROVI          PIC S9(13)V9(4) USAGE COMP-3.      00031500
030200       15 REC-FEULPROV         PIC X(10).                         00031600
030300     10 REC-RETENCIONES.                                          00031700
030400       15 REC-FERETEN          PIC X(10).                         00031800
030500       15 REC-SECURET          PIC S9(5)V USAGE COMP-3.           00031900
030600       15 REC-UGIRETEN         PIC S9(13)V9(4) USAGE COMP-3.      00032000
030700     10 REC-UGNPLPEN           PIC S9(5)V USAGE COMP-3.           00032100
030800     10 REC-COBR-EXTERNA.                                         00032200
030900       15 REC-COD-EMPCOBEX     PIC X(8).                          00032300
031000       15 REC-FEC-ENVCOBEX     PIC X(10).                         00032400
031100       15 REC-CODCONLI-COBEX   PIC X(3).                          00032500
031200       15 REC-IMP-COMCOBEXINI  PIC S9(13)V9(4) USAGE COMP-3.      00032600
031300       15 REC-IMP-COMCOBEXREC  PIC S9(13)V9(4) USAGE COMP-3.      00032700
031400       15 REC-FEC-BAJCOBEX     PIC X(10).                         00032800
031500       15 REC-IND-DESGCOBE     PIC X(1).                          00032900
031600     10 REC-MORA.                                                 00033000
031700       15 REC-CODCONLI-MOR     PIC X(3).                          00033100
031800       15 REC-IMP-MORCAL       PIC S9(13)V9(4) USAGE COMP-3.      00033200
031900       15 REC-IMP-MORREC       PIC S9(13)V9(4) USAGE COMP-3.      00033300
032000       15 REC-IND-DESGMOR      PIC X(1).                          00033400
032100       15 REC-TAS-MOR          PIC S9(3)V9(6) USAGE COMP-3.       00033500
032200       15 REC-IND-CAMB-TASA-MOR                                   00033600
032300                               PIC X(1).                          00033700
032400     10 REC-COMPENSATOR.                                          00033800
032500       15 REC-CODCONLI-CPS     PIC X(3).                          00033900
032600       15 REC-IMP-CPSCAL       PIC S9(13)V9(4) USAGE COMP-3.      00034000
032700       15 REC-IMP-CPSREC       PIC S9(13)V9(4) USAGE COMP-3.      00034100
032800       15 REC-IND-DESGCPS      PIC X(1).                          00034200
032900       15 REC-TAS-CPS          PIC S9(3)V9(6) USAGE COMP-3.       00034300
033000       15 REC-FEC-CALCPS       PIC X(10).                         00034400
033100       15 REC-IND-CAMB-TASA-CPS                                   00034500
033200                               PIC X(1).                          00034600
033300     10 REC-DEUDA.                                                00034700
033400       15 REC-SITDEUCT         PIC X(2).                          00034800
033500       15 REC-FEC-SITCONT      PIC X(10).                         00034900
033600     10 REC-TIPOTIT          PIC S9(3)V9(6) USAGE COMP-3.         00035000
033700     10 REC-TIPOTOT          PIC S9(3)V9(6) USAGE COMP-3.         00035100
033800     10 REC-UGYESCON         PIC X(1).                            00035200
033900       88 REC-88-VAL-UGYESCON         VALUE '0', '1', '2', '3'.   00035300
034000       88 REC-88-UGYESCON-NCONT       VALUE '0'.                  00035400
034100       88 REC-88-UGYESCON-INTPRO      VALUE '1'.                  00035500
034200       88 REC-88-UGYESCON-INTCUE      VALUE '2'.                  00035600
034300       88 REC-88-UGYESCON-NVAL        VALUE '3'.                  00035700
034400     10 REC-UGYCVINT         PIC X(8).                            00035800
034500       88 REC-88-VAL-UGYCVINT         VALUE '0', '1'.             00035900
034600       88 REC-88-UGYCVINT-NCAMB       VALUE '0'.                  00036000
034700       88 REC-88-UGYCVINT-CAMBI       VALUE '1'.                  00036100
034800     10 REC-FECALMOR         PIC X(10).                           00036200
034900     10 REC-FEC-HASTAMOR     PIC X(10).                           00036300
035000     10 REC-IND-MORACOND     PIC X(1).                            00036400
035100     10 REC-CAPITALIZ.                                            00036500
035200        15 REC-IMP-CAPITALIZ    PIC S9(13)V9(4) USAGE COMP-3.     00036600
035300        15 REC-CODCONLI-CPZ     PIC X(3).                         00036700
035400     10 REC-DIFERIDOS.                                            00036800
035500        15 REC-IMP-DIFERIDO     PIC S9(13)V9(4) USAGE COMP-3.     00036900
035600        15 REC-CODCONLI-DIF     PIC X(3).                         00037000
035700     10 REC-BALLOOM.                                              00037100
035800        15 REC-IMP-BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.     00037200
035900        15 REC-CODCONLI-BAL     PIC X(3).                         00037300
036000        15 REC-SDO-BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.     00037400
036100        15 REC-BALLOOM-FAC      PIC S9(13)V9(4) USAGE COMP-3.     00037500
036200     10 REC-TIP-CAMB-FELIQ      PIC S9(6)V9(5)  USAGE COMP-3.     00037600
036300     10 REC-IND-PAGO-ANTIC      PIC X(1).                         00037700
036400     10 REC-INT-ORDEN           PIC S9(13)V9(4) COMP-3.           00037800
036500     10 REC-REA-ORDEN           PIC S9(13)V9(4) COMP-3.           00037900
036600     10 REC-EJERCICIO-CAST      PIC X(10).                        00038000
036700     10 REC-IND-SEGCESAN        PIC X(1).                         00038100
036800   05 REC-MODIF-IMPLANTACION.                                     00038200
036900     10 REC-MORA-2.                                               00038300
037000         15 REC-TIP-TMC-APL     PIC X(1).                         00038400
037100     10 REC-COBRANZA-EXTERNA-2.                                   00038500
037200         15 REC-IND-LIQ-COBEX   PIC X(1).                         00038600
037300     10 REC-VARIOS.                                               00038700
037400         15 REC-IMP-CAST-CUOTA  PIC S9(13)V9(4) USAGE COMP-3.     00038800
037500         15 REC-IND-CAST-TOT    PIC X(1).                         00038900
         05 REC-MODIF-INDEXACION.                                       00039000
           10 REC-COEFICI-INDEX       PIC X(4).                         00039100
           10 REC-FACTOR-INDEX        PIC S9(03)V9(6) USAGE COMP-3.     00039200
           10 REC-IMPAJUST-SAL        PIC S9(13)V9(4) USAGE COMP-3.     00039300
           10 REC-CODCONLI-AJUSAL     PIC X(3).                         00039400
           10 REC-IMPAJUST-CAP        PIC S9(13)V9(4) USAGE COMP-3.     00039500
           10 REC-CODCONLI-AJUCAP     PIC X(3).                         00039600
           10 REC-IMPAJUST-INT        PIC S9(13)V9(4) USAGE COMP-3.     00039700
           10 REC-CODCONLI-AJUINT     PIC X(3).                         00039800
037600                                                                  00039900
038200   05 REC-STAMP.                                                  00040000
038300     10 REC-ENTIDAD-UMO         PIC X(4).                         00040100
038400     10 REC-CENTRO-UMO          PIC X(4).                         00040200
038500     10 REC-USERID-UMO          PIC X(8).                         00040300
038600     10 REC-NETNAME-UMO         PIC X(8).                         00040400
038700     10 REC-TIMESTAMP           PIC X(26).                        00040500
                                                                        00040510
      *ISBAN-000X-I                                                     00040520
           10 REC-CFTNA               PIC S9(03)V9(6) USAGE COMP-3.     00040600
           10 REC-CFTNA-SIMP          PIC S9(03)V9(6) USAGE COMP-3.     00040700
      *ISBAN-000X-F                                                     00040800
