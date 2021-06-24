000100******************************************************************00000100
000200*                       B S C H - ALTAIR                         *00000200
000300* -------------------------------------------------------------- *00000300
000400* ¡REA       - ACTIVO                                            *00000400
000500* APLICACI”N - PR…STAMOS Y AVALES                                *00000500
000600* -------------------------------------------------------------- *00000600
000700* REGISTRO          - UGTCPRO                                    *00000700
000800* FECHA DE CREACI”N - 23-NOV-1999                                *00000800
000900*                                                                *00000900
001000* COPY DE CARACTERISTICAS GENERALES DEL PRODUCTO                 *00001000
001100*                                                                *00001100
001200******************************************************************00001200
001300*                     LOG DE MODIFICACIONES                      *00001300
001400* -------------------------------------------------------------- *00001400
001500* FECHA    | AUTOR     | DESCRIPCI”N                             *00001500
001600* -------------------------------------------------------------- *00001600
001700*                                                                *00001700
      ******************************************************************00001800
      *  MODIFICACION: SE INCORPORAN LOS CAMPOS CATEGORIA DE REESTRUC- *00001900
      *                TURACION Y SITUACION CONTABLE                   *00002000
      *  RESPONSABLE : TELVENT                                         *00002100
      *  FECHA       : FEBRERO 2011                                    *00002200
      *  PETICION    : ALT-10-05639-0A                                 *00002300
      *  ID CAMBIO   : ISBAN-001                                       *00002400
      ******************************************************************00002500
001800******************************************************************00002600
001900*                                                                *00002700
002000* CAMPO                      DESCRIPCIÛN                         *00002800
002100* -------------------------- ----------------------------------- *00002900
002200* CLAVE.                                                         *00003000
002300*   ENTIDAD ---------------- CODIGO DE ENTIDAD                   *00003100
002400*   PRODUCTO --------------- CODIGO DE PRODUCTO                  *00003200
002500*   SUBPRO ----------------- CODIGO DE SUBPRODUCTO               *00003300
002600* DATOS.                                                         *00003400
002700*   FECOVALI --------------- FECHA INICIO DE VALIDEZ             *00003500
002800*   FEFIVALI --------------- FECHA FIN DE VALIDEZ                *00003600
002900*   COD-ESTPROD ------------ CODIGO ESTADO DEL PRODUCTO          *00003700
003000*                            'A'- APERTURA                       *00003800
003100*                            'D'- DESACTIVADO                    *00003900
003200*                            'O'- OPERATIVO                      *00004000
003300*   TEXTORED ---------------  DESCRIPCION ABREVIADA DEL SUBPROD. *00004100
003400*   TEXTOLAR ---------------  DESCRIPCION LARGA DEL SUBPRODUCTO  *00004200
003500*   COD-DIVISA -------------  CODIGO DE DIVISA                   *00004300
003600*   TIP-PRODUCTO -----------  TIPO DE PRODUCTO                   *00004400
003700*   COD-GRUPOPRO -----------  CODIGO AGRUPACION DE PRODUCTOS     *00004500
003800*                             USUARIO SS.CC                      *00004600
003900*   IND-MOD-OPER -----------  INDICADOR DE MODALIDAD DE OPERACION*00004700
004000*                             DEL PRODUCTO.                      *00004800
004100*                            'N'- NORMAL                         *00004900
004200*                            'L'- LINEA                          *00005000
004300*                            'F'- FLEXIBLE                       *00005100
004400*                            'D'- DISPOSICION LINEA              *00005200
004500*   NIVCONCE ---------------  NIVEL DE CONCESION (0-9)           *00005300
004600*   IND-CTAOBLIG -----------  INDICADOR DE CUENTA OBLIGATORIA    *00005400
004700*   UGQFIMIN ---------------  CANTIDAD MINIMA DE FIADORES        *00005500
004800*   UGYPREMP ---------------  PRESTAMOS A EMPLEADOS              *00005600
004900*                            'S'- SOLO PARA EMPLEADOS            *00005700
005000*                            'N'- SOLO PARA NO EMPLEADOS         *00005800
005100*                            'I'- INDIFERENTE                    *00005900
005200*   IND-CLASEPRO -----------  INDICADOR DE CLASE DE PRODUCTO     *00006000
005300*                             PARA DIFERENCIAR AVALES Y PRESTAMOS*00006100
005400*   IND-FINIQUIT ----------- INDICADOR DE PRESENTAR DOCUMENTO    *00006200
005500*                            DE FINIQUITO DE LA FIANZA. EN VENCI *00006300
005600*                            MIENTO DEL AVAL.EVENTO=CANCELACION. *00006400
005700*                            (S/N).                              *00006500
005800*   IND-SUJREVCO ----------- INDICADOR DE SUJETO A REVISION DE   *00006600
005900*                            CONTROL (S/N).                      *00006700
006000*   IND-CANCAUTO ----------- INDICADOR DE CANCELACION AUTOMATICA *00006800
006100*                            PARA AVALES (S/N).                  *00006900
006200*   IND-EJECFORZ  ---------- INDICADOR DE EJECUCION AVAL FORZADA *00007000
006300*   NUM-DIAS-PAGOEJ--------- DIAS PARA EL PAGO DE EJECUCION AVAL *00007100
006400*   COD-PRELACION ---------- CLAVE PARA TABLA DE PRELACIONES     *00007200
006500*   MODCOBRO  -------------- INDICADOR DE MODALIDAD DE COBRO     *00007300
006600*                            '1'-DEMORA COMO CONCEPTO            *00007400
006700*                            '2'-CADA CONCEPTO CON SU MORA       *00007500
006800*                            '3'-NO EXISTE                       *00007600
006900*   PLZ-MIN-ENTREGAS ------  PLAZO MINIMO OBLIGATORIO ENTRE      *00007700
007000*                            ENTREGAS.                           *00007800
007100*   IND-REVOLVENTE---------  INDICADOR DE PRODUCTO REVOLVENTE    *00007900
007200*   LIMITES.                                                     *00008000
007300*     IMPMAXGE ------------  IMPORTE MAXIMO GENERAL              *00008100
007400*     DIV-IMPMAXGE---------  DIVISA ALTERNAT IMPORTE MAXIMO GRAL *00008200
007500*     IMP-MINGEN ----------  IMPORTE MINIMO GENERAL              *00008300
007600*     POR-MAXCONC ---------  PORCENTAJE MAXIMO DE CONCESION      *00008400
007700*     POR-MAXCUOT ---------  PORCENTAJE MINIMO DE CONCESION      *00008500
007800*     IMP-MININGR ---------  MONTO MINIMO DE INGRESOS NETOS      *00008600
007900*     NUM-EDADMIN ---------  EDAD MINIMA                         *00008700
008000*     NUM-EDADMAX ---------  EDAD MAXIMA                         *00008800
008100*     CUPOTOPR ------------  CUPO TOTAL POR PRODUCTO             *00008900
008200*     CUPODIPR ------------  CUPO DISPUESTO POR PRODUCTO         *00009000
008300*     PLAZOMIN ------------  PLAZO MINIMO DEL PRESTAMO           *00009100
008400*     PLAZOMAX ------------  PLAZO MAXIMO DEL PRESTAMO           *00009200
008500*     PLAZOMAX-RENOV ------  PLAZO MAXIMO INCLUIDAS RENOVACIONES *00009300
008600*     UGCCAMIN ------------  CODIGO DE CARENCIA MINIMA           *00009400
008700*     UGCCAMAX ------------  CODIGO DE CARENCIA MAXIMA           *00009500
008800*     NUM-DIAS-ABONO-------  DIAS MAXIMOS DESDE FORMALIZACION    *00009600
008900*                            A ABONO                             *00009700
009000*   CUPOS.                                                       *00009800
009100*     INDCUOFI ------------  INDICADOR DE CUPO POR OFICINA       *00009900
009200*                            '0'-NO SE DISTRIBUYE POR OFICINA    *00010000
009300*                            '1'-PENDIENTE DE DISTRIBUIR POR     *00010100
009400*                               PROCESO BATCH.                   *00010200
009500*                            '2'-YA DISTRIBIDO POR PROCESO BATCH *00010300
009600*     IND-CUPOREVO --------  INDICADOR DE CUPO REVOLVENTE (S/N)  *00010400
009700*   COBROS.                                                      *00010500
009800*     IND-COBPARC ---------  INDICADOR DE COBRO PARCIAL PERMITIDO*00010600
009900*                            'N'-NO SE PERMITE COBRO PARCIAL     *00010700
010000*                            'S'-SE PERMITE COBRO PARCIAL        *00010800
010100*                            'I'-SOLO PERMITE COBRO PARCIAL      *00010900
010200*                                CUOTAS IMPAGADAS.               *00011000
010300*     IMPMIPAR ------------  IMPORTE MINIMO DE PAGO DE RECIBOS   *00011100
010400*     IND-CUOANTIC --------  INDICADOR DE ADMITE CUOTAS ANTICIPA_*00011200
010500*                            DAS (S/N).                          *00011300
010600*     IND-APLRETEN --------  INDICADOR DE APLICAR RETENCION (S/N)*00011400
010700*     IND-CARGOAUT --------  INDICADOR DE EJECUCI”N CARGO AUTOMA *00011500
010800*   DISPOSICIONES.                                               *00011600
010900*     INDISSU -------------  INDICADOR DE DISPOSICIONES SUCESIVAS*00011700
011000*                            'N'-NO SE PERMITEN                  *00011800
011100*                            'S'-SI SE PERMITEN                  *00011900
011200*     NUM-MAXDISPO --------  NUMERO MAXIMO DE DISPOS. SUCESIVAS  *00012000
011300*     PORDISPO ------------  PORCENTAJE DE DISPOSICIONES SUCESIV.*00012100
011400*     IMP-MINDISPO --------  IMPORTE MINIMO PARA DISPOSICIONES   *00012200
011500*                            SUCESIVAS.                          *00012300
011600*     IMP-MAXDISPO --------  IMPORTE MAXIMO PARA DISPOSICIONES   *00012400
011700*                            SUCESIVAS.                          *00012500
011800*     IND-RENOVPLA --------  INDICADOR DE RENOVAR PLAZO EN DISPO_*00012600
011900*                            SICIONES SUCESIVAS (S/N).           *00012700
012000*   ENTREGAS.                                                    *00012800
012100*     INDENTAN ------------  INDICADOR DE ENTREGAS ANTICIPADAS   *00012900
012200*                            'N'-NO SE PERMITEN                  *00013000
012300*                            'S'-SI SE PERMITEN                  *00013100
012400*     IND-MINENTANTIC -----  INDICA COMO SE EXPRESA EL MINIMO EN *00013200
012500*                            ENTREGAS ANTICIPADAS                *00013300
012600*                            'P'-PROCENTAJE                      *00013400
012700*                            'I'-IMPORTE                         *00013500
012800*                            'C'-NUMERO DE CUOTAS                *00013600
012900*                            'D'-NUMERO DE DIAS                  *00013700
013000*     IMPMINEA ------------  IMPORTE MINIMO DE ENTREGAS ANTICIP. *00013800
013100*     POR-MINEA -----------  MINIMO DE ENTREGAS ANTICIPADAS      *00013900
013200*                            EXPRESADOS EN PORCENTAJE            *00014000
013300*     NUM-MINEA -----------  MINIMO DE ENTREGAS ANTICIPADAS      *00014100
013400*                            EXPRESADO EN NUMERO DE DIAS O CUOTAS*00014200
013500*   IMP-VOLUNTARI.                                               *00014300
013600*     IND-IMPAVOL ---------  IMPAGO VOLUNTARIO                   *00014400
013700*                            'M'-MANUAL                          *00014500
013800*                            'A'-AUTOMATICO                      *00014600
013900*                            'N'-NO SE ADMITE                    *00014700
014000*   PRORROGA.                                                    *00014800
014100*     IND-ADMPRORR -------- INDICADOR DE ADMITE PRORROGAS (S/N)  *00014900
014200*   RENOVACION.                                                  *00015000
014300*     IND-ADMRENOV -------- INDICADOR ADMITE RENOVACION (S/N)    *00015100
014400*   COND-ESPECIAL.                                               *00015200
014500*     IND-CONDES  --------- INDICADOR ADMITE CONDICIONES ESPECI_ *00015300
014600*                           FICAS (S/N)                          *00015400
014700*     IND-ADMCANAL -------- INDICADOR ADMITE CANAL (S/N)         *00015500
014800*     IND-ADMSEGME -------- INDICADOR ADMITE SEGMENTO (S/N)      *00015600
014900*     IND-ADMSUBVE -------- INDICADOR ADMITE SUBVENCIONES (S/N)  *00015700
015000*     IND-ADMEMPRE -------- INDICADOR ADMITE CONDICIONES ESPECI_ *00015800
015100*                           ALES POR CONVENIO CON EMPRESA (S/N)  *00015900
015200*     IND-ADMAGENT -------- INDICADOR ADMITE CONDICIONES ESPECI_ *00016000
015300*                           ALES POR AGENTE               (S/N)  *00016100
015400*     IND-ADMPAQUE -------- INDICADOR ADMITE CONDICIONES ESPECI_ *00016200
015500*                           ALES POR PAQUETE              (S/N)  *00016300
015600*     IND-ADMCAMPA -------- INDICADOR ADMITE CAMPA#A(S/N)        *00016400
015700*   COBR-EXTERNA.                                                *00016500
015800*     IND-COBRZAEX -------- INDICADOR ADMITE COBRANZA EXTERNA    *00016600
015900*                           (S/N)                                *00016700
016000*     IND-CARAUT-CBE ------ INDICADOR CARGO ATOMATICO SI RECIBOS *00016800
016100*                           EN COBRANZA EXTERNA (S/N)            *00016900
016200*   ARRENDAM.                                                    *00017000
016300*     IND-ARRENDAM -------- INDICADOR ARRENDAMIENTO FINANCIERO   *00017100
016400*                           'N'- NO ADMITE                       *00017200
016500*                           'I'- IMPORTE                         *00017300
016600*                           'P'- PORCENTAJE                      *00017400
016700*   PAGARE.                                                      *00017500
016800*     IMP-MINTIMBR --------  IMPORTE MINIMO PARA COBRO DE TIMBRE *00017600
016900*                           EN PAGARES                           *00017700
017000*   LIN-FLEXIBLES.                                               *00017800
017100*     IMP-MINGIRO --------- MONTO MINIMO DE GIRO POR CAJA PARA   *00017900
017200*                           LINEAS FLEXIBLES                     *00018000
017300*     IMP-MAXCUOTA -------- IMPORTE MAXIMO DE LA CUOTA           *00018100
017400*     NUM-CUOMIN ---------- NUMERO MINIMO DE CUOTAS PARA DISPOSI_*00018200
017500*                           SICION EN LINEAS FLEXIBLES           *00018300
017600*     NUM-CUOMAX ---------- NUMERO MAXIMO DE CUOTAS PARA DISPOSI_*00018400
017700*                           SICION EN LINEAS FLEXIBLES           *00018500
017800*     NUM-CUORED ---------- NUMERO DE CUOTAS PACTADAS PARA DISPO_*00018600
017900*                           SICIONES POR RED EXTERNA.            *00018700
018000*   OTROS-INDICAD.                                               *00018800
018100*     IND-CAPINTER -------- INDICADOR DE CAPITALIZAR INTERESES   *00018900
018200*        <'S'>              CAPITALIZA                           *00019000
018300*        <'N'>              NO CAPITALIZA                        *00019100
018400*        <'D'>              DIFIERE (CHILENOS)                   *00019200
018500*     IND-ADMCUEXT --------  INDICADOR DE ADMITE CUOTAS EXTRAOR_ *00019300
018600*                           DINARIAS.                            *00019400
018700*     IND-PREMIO ---------- INDICADOR DE SI EL PRODUCTO ADMITE   *00019500
018800*     IND-DESGL-MVTOS------ IND. DESGLOSE DE MOVIMIENTOS         *00019600
018900*   PRO-DATOS-PAGO-ADICIONAL.                                    *00019700
019000*     IND-CADIC------------ SI SE REPITEN PAGOS ADICIONALES      *00019800
019100*     NUM-MAX-PAG-ADI------ NUMERO MAXIMO DE PAGOS ADICIONALES   *00019900
019200*     POR-MAX-PAG-ADI------ PORCENTAJE MAXIMO PERMITIDO          *00020000
019300*     DIA-FACAN------------ DIA DE FACTURACI”N ANTICIPADA        *00020100
019400*     IND-CALCUO-DOS-T      IND CALCULO CUOTAS A DOS TASAS      E*00020200
019500*   PRO-DATOS-MUTUOS        .                                    *00020300
019600*     IND-ENDOSO----------- INDICADOR DE ENDOSO                  *00020400
019700*   PRO-DATOS-ROTATIVOS                                          *00020500
019800*     SALDO-MIN-AUTOM------ SALDO MINIMO A PARTIR PARA FACT TOTAL*00020600
019900*   PRO-OTROS-DATOS                                              *00020700
020000*     NUMDEC--------------- N˙MERO DE DECIMALES                  *00020800
      *ISBAN-0001-I                                                     00020900
      *    10 PRO-COD-CTG-RTT---- CODIGO CATEGORIA REESTRUCTURADO       00021000
      *    10 PRO-COD-STC-CTB---- CODIGO SITUACION CONTABLE             00021100
      *ISBAN-0001-F                                                     00021200
020100*                                                                 00021300
      *ISBAN-0002-I                                                    *00021400
ID6797*    10 PRO-IND-INDEXADO----INDICADOR DE INDEXADO                *00021500
ID6797*    10 PRO-COEFICI-INDEX---CODIGO COEFICIENTE INDEXADO          *00021600
ID6797*    10 PRO-COEFICI-VISUAL--CODIGO COEFICIENTE VISUALIZACION     *00021700
      *ISBAN-0002-F                                                    *00021800
020800*                                                                *00021900
020900* STAMP.                                                         *00022000
021000*   ENTIDAD-UMO ----------- ENTIDAD ULTIMA MODIFICACION          *00022100
021100*   CENTRO-UMO ------------ CENTRO ULTIMA MODIFICACION           *00022200
021200*   USERID-UMO ------------ USUARIO ULTIMA MODIFICACION          *00022300
021300*   NETNAME-UMO ----------- TERMINAL ULTIMA MODIFICACION         *00022400
021400*   TIMEST-UMO ------------ FECHA Y HORA ULTIMA MODIFICACION     *00022500
021500******************************************************************00022600
021600 02  UGTCPRO.                                                     00022700
021700   05 PRO-CLAVE.                                                  00022800
021800     10 PRO-ENTIDAD          PIC X(4).                            00022900
021900     10 PRO-PRODUCTO         PIC X(2).                            00023000
022000     10 PRO-SUBPRO           PIC X(4).                            00023100
022100   05 PRO-DATOS.                                                  00023200
022200     10 PRO-FECOVALI         PIC X(10).                           00023300
022300     10 PRO-FEFIVALI         PIC X(10).                           00023400
022400     10 PRO-COD-ESTPROD      PIC X(1).                            00023500
022500       88 PRO-88-VAL-ESTAD             VALUE 'A', 'D', 'O'.       00023600
022600         88 PRO-88-VAL-APR             VALUE 'A'.                 00023700
022700         88 PRO-88-VAL-DES             VALUE 'D'.                 00023800
022800         88 PRO-88-VAL-OPER            VALUE 'O'.                 00023900
022900     10 PRO-TEXTORED         PIC X(15).                           00024000
023000     10 PRO-TEXTOLAR         PIC X(50).                           00024100
023100     10 PRO-COD-DIVISA       PIC X(3).                            00024200
023200     10 PRO-TIP-PRODUCTO     PIC X(2).                            00024300
023300     10 PRO-COD-GRUPOPRO     PIC X(4).                            00024400
023400     10 PRO-IND-MOD-OPER     PIC X(1).                            00024500
023500       88 PRO-88-VAL-MODOPER          VALUE 'N', 'L', 'F', 'D'.   00024600
023600       88 PRO-88-VAL-MDOP-NOR         VALUE 'N'.                  00024700
023700       88 PRO-88-VAL-MDOP-LIN         VALUE 'L'.                  00024800
023800       88 PRO-88-VAL-MDOP-FLX         VALUE 'F'.                  00024900
023900       88 PRO-88-VAL-MDOP-DLI         VALUE 'D'.                  00025000
024000     10 PRO-NIVCONCE         PIC X(1).                            00025100
024100     10 PRO-IND-CTAOBLIG     PIC X(1).                            00025200
024200     10 PRO-UGQFIMIN         PIC S9(3)V USAGE COMP-3.             00025300
024300     10 PRO-UGYPREMP         PIC X(1).                            00025400
024400     10 PRO-IND-CLASEPRO     PIC X(1).                            00025500
024500     10 PRO-IND-FINIQUIT     PIC X(1).                            00025600
024600     10 PRO-IND-SUJREVCO     PIC X(1).                            00025700
024700     10 PRO-IND-CANCAUTO     PIC X(1).                            00025800
024800     10 PRO-IND-EJECFORZ     PIC X(1).                            00025900
024900     10 PRO-NUM-DIAS-PAGOEJ  PIC S9(3)V USAGE COMP-3.             00026000
025000     10 PRO-COD-PRELACION    PIC X(3).                            00026100
025100     10 PRO-MODCOBRO         PIC X(1).                            00026200
025200       88 PRO-88-VAL-MODCOBR          VALUE '1', '2', '3'.        00026300
025300       88 PRO-88-MODCBR-DCON          VALUE '1'.                  00026400
025400       88 PRO-88-MODCBR-CONC          VALUE '2'.                  00026500
025500       88 PRO-88-MODCBR-INEX          VALUE '3'.                  00026600
025600     10 PRO-PLZ-MIN-ENTREGAS PIC X(4).                            00026700
025700     10 PRO-IND-REVOLVENTE   PIC X(1).                            00026800
025800     10 PRO-LIMITES.                                              00026900
025900       15 PRO-IMPMAXGE         PIC S9(13)V9(4) USAGE COMP-3.      00027000
026000       15 PRO-DIV-IMPMAXGE     PIC X(3).                          00027100
026100       15 PRO-IMP-MINGEN       PIC S9(13)V9(4) USAGE COMP-3.      00027200
026200       15 PRO-POR-MAXCONC      PIC S9(3)V9(6) USAGE COMP-3.       00027300
026300       15 PRO-POR-MAXCUOT      PIC S9(3)V9(6) USAGE COMP-3.       00027400
026400       15 PRO-IMP-MININGR      PIC S9(13)V9(4) USAGE COMP-3.      00027500
026500       15 PRO-NUM-EDADMIN      PIC S9(3)V USAGE COMP-3.           00027600
026600       15 PRO-NUM-EDADMAX      PIC S9(3)V USAGE COMP-3.           00027700
026700       15 PRO-CUPOTOPR         PIC S9(13)V9(4) USAGE COMP-3.      00027800
026800       15 PRO-CUPODIPR         PIC S9(13)V9(4) USAGE COMP-3.      00027900
026900       15 PRO-PLAZOMIN         PIC X(4).                          00028000
027000       15 PRO-PLAZOMAX         PIC X(4).                          00028100
027100       15 PRO-PLAZOMAX-RENOV   PIC X(4).                          00028200
027200       15 PRO-UGCCAMIN         PIC X(4).                          00028300
027300       15 PRO-UGCCAMAX         PIC X(4).                          00028400
027400       15 PRO-NUM-DIAS-ABONO   PIC S9(3)V USAGE COMP-3.           00028500
027500       15 PRO-IND-AMPLAVAL     PIC X(1).                          00028600
027600     10 PRO-CUPOS.                                                00028700
027700       15 PRO-INDCUOFI         PIC X(1).                          00028800
027800           88 PRO-88-VAL-CUPOFI           VALUE '0', '1', '2'.    00028900
027900           88 PRO-88-CUPOFI-NDIS          VALUE '0'.              00029000
028000           88 PRO-88-CUPOFI-PEND          VALUE '1'.              00029100
028100           88 PRO-88-CUPOFI-DIST          VALUE '2'.              00029200
028200       15 PRO-IND-CUPOREVO     PIC X(1).                          00029300
028300     10 PRO-COBROS.                                               00029400
028400       15 PRO-IND-COBPARC      PIC X(1).                          00029500
028500         88 PRO-88-VAL-INDCOBPAR        VALUE 'N', 'S', 'I'.      00029600
028600         88 PRO-88-COBPARC-NO           VALUE 'N'.                00029700
028700         88 PRO-88-COBPARC-SI           VALUE 'S'.                00029800
028800         88 PRO-88-COBPARC-CUOIMP       VALUE 'I'.                00029900
028900       15 PRO-IMPMIPAR         PIC S9(13)V9(4) USAGE COMP-3.      00030000
029000       15 PRO-IND-CUOANTIC     PIC X(1).                          00030100
029100       15 PRO-IND-APLRETEN     PIC X(1).                          00030200
029200       15 PRO-IND-CARGOAUT     PIC X(1).                          00030300
029300         88 PRO-88-IND-CARGOAUT-TODOS   VALUE 'T'.                00030400
029400         88 PRO-88-IND-CARGOAUT-HABIL   VALUE 'H'.                00030500
029500     10 PRO-DISPOSICIONES.                                        00030600
029600       15 PRO-INDISSU          PIC X(1).                          00030700
029700       15 PRO-NUM-MAXDISPO     PIC S9(3)V USAGE COMP-3.           00030800
029800       15 PRO-PORDISPO         PIC S9(3)V9(6) USAGE COMP-3.       00030900
029900       15 PRO-IMP-MINDISPO     PIC S9(13)V9(4) USAGE COMP-3.      00031000
030000       15 PRO-IMP-MAXDISPO     PIC S9(13)V9(4) USAGE COMP-3.      00031100
030100       15 PRO-IND-RENOVPLA     PIC X(1).                          00031200
030200     10 PRO-ENTREGAS.                                             00031300
030300       15 PRO-INDENTAN         PIC X(1).                          00031400
030400       15 PRO-IND-MINENTANTIC  PIC X(1).                          00031500
030500         88 PRO-88-VAL-MINENTANTIC      VALUE 'P', 'I', 'C', 'D'. 00031600
030600         88 PRO-88-MINENTANTIC-POR      VALUE 'P'.                00031700
030700         88 PRO-88-MINENTANTIC-IMP      VALUE 'I'.                00031800
030800         88 PRO-88-MINENTANTIC-NUMCUO   VALUE 'C'.                00031900
030900         88 PRO-88-MINENTANTIC-NUMDIA   VALUE 'D'.                00032000
031000       15 PRO-IMPMINEA         PIC S9(13)V9(4) USAGE COMP-3.      00032100
031100       15 PRO-POR-MINEA        PIC S9(3)V9(6) USAGE COMP-3.       00032200
031200       15 PRO-NUM-MINEA        PIC S9(3)V USAGE COMP-3.           00032300
031300     10 PRO-IMP-VOLUNTARI.                                        00032400
031400       15 PRO-IND-IMPAVOL      PIC X(1).                          00032500
031500         88 PRO-88-VAL-IMPAVOL          VALUE 'M', 'A', 'N'.      00032600
031600         88 PRO-88-IMPAVOL-MAN          VALUE 'M'.                00032700
031700         88 PRO-88-IMPAVOL-AUT          VALUE 'A'.                00032800
031800         88 PRO-88-IMPAVOL-NO           VALUE 'N'.                00032900
031900     10 PRO-PRORROGA.                                             00033000
032000       15 PRO-IND-ADMPRORR     PIC X(1).                          00033100
032100     10 PRO-RENOVACION.                                           00033200
032200       15 PRO-IND-ADMRENOV     PIC X(1).                          00033300
032300     10 PRO-COND-ESPECIAL.                                        00033400
032400       15 PRO-IND-CONDES       PIC X(1).                          00033500
032500       15 PRO-IND-ADMCANAL     PIC X(1).                          00033600
032600       15 PRO-IND-ADMSEGME     PIC X(1).                          00033700
032700       15 PRO-IND-ADMSUBVE     PIC X(1).                          00033800
032800       15 PRO-IND-ADMEMPRE     PIC X(1).                          00033900
032900       15 PRO-IND-ADMAGENT     PIC X(1).                          00034000
033000       15 PRO-IND-ADMPAQUE     PIC X(1).                          00034100
033100       15 PRO-IND-ADMCAMPA     PIC X(1).                          00034200
033200     10 PRO-COBR-EXTERNA.                                         00034300
033300       15 PRO-IND-COBRZAEX     PIC X(1).                          00034400
033400       15 PRO-IND-CARAUT-CBE   PIC X(1).                          00034500
033500     10 PRO-ARRENDAM.                                             00034600
033600       15 PRO-IND-ARRENDAM     PIC X(1).                          00034700
033700         88 PRO-88-VAL-ARRENDAM         VALUE 'N', 'I', 'P'.      00034800
033800         88 PRO-88-ARRENDAN-NO          VALUE 'N'.                00034900
033900         88 PRO-88-ARRENDAN-IMP         VALUE 'I'.                00035000
034000         88 PRO-88-ARRENDAN-POR         VALUE 'P'.                00035100
034100     10 PRO-PAGARE.                                               00035200
034200       15 PRO-IMP-MINTIMBR     PIC S9(13)V9(4) USAGE COMP-3.      00035300
034300     10 PRO-LIN-FLEXIBLES.                                        00035400
034400       15 PRO-IMP-MINGIRO      PIC S9(13)V9(4) USAGE COMP-3.      00035500
034500       15 PRO-IMP-MAXCUOTA     PIC S9(13)V9(4) USAGE COMP-3.      00035600
034600       15 PRO-NUM-CUOMIN       PIC S9(7)V USAGE COMP-3.           00035700
034700       15 PRO-NUM-CUOMAX       PIC S9(7)V USAGE COMP-3.           00035800
034800       15 PRO-NUM-CUORED       PIC S9(7)V USAGE COMP-3.           00035900
034900     10 PRO-OTROS-INDICAD.                                        00036000
035000       15 PRO-IND-CAPINTER     PIC X(1).                          00036100
035100       15 PRO-IND-ADMCUEXT     PIC X(1).                          00036200
035200       15 PRO-IND-PREMIO       PIC X(1).                          00036300
035300       15 PRO-IND-DESGL-MVTOS  PIC X(1).                          00036400
035400     10 PRO-DATOS-PAGO-ADICIONAL.                                 00036500
035500        15 PRO-IND-CADIC       PIC X(1).                          00036600
035600        15 PRO-NUM-MAX-PAG-ADI PIC S9(4)V USAGE COMP-3.           00036700
035700        15 PRO-POR-MAX-PAG-ADI PIC S9(3)V9(6) USAGE COMP-3.       00036800
035800     10 PRO-DIA-FACAN        PIC S9(3) COMP-3.                    00036900
035900     10 PRO-IND-CALCUO-DOS-T PIC X(1).                            00037000
036000     10 PRO-DATOS-MUTUOS.                                         00037100
036100        15 PRO-IND-ENDOSO      PIC X(1).                          00037200
036200     10 PRO-DATOS-ROTATIVOS.                                      00037300
036300        15 PRO-SALDO-MIN-AUTOM PIC S9(13)V9(4) COMP-3.            00037400
036400     10 PRO-OTROS-DATOS.                                          00037500
036500        15 PRO-NUMDEC          PIC S9(1) COMP-3.                  00037600
      *ISBAN-0001-I                                                     00037700
           10 PRO-COD-CTG-RTT      PIC  X(02).                          00037800
           10 PRO-COD-STC-CTB      PIC  X(02).                          00037900
      *ISBAN-0001-F                                                     00038000
036600                                                                  00038100
037300   05 PRO-STAMP.                                                  00038200
037400     10 PRO-ENTIDAD-UMO      PIC X(4).                            00038300
037500     10 PRO-CENTRO-UMO       PIC X(4).                            00038400
037600     10 PRO-USERID-UMO       PIC X(8).                            00038500
037700     10 PRO-NETNAME-UMO      PIC X(8).                            00038600
037800     10 PRO-TIMEST-UMO       PIC X(26).                           00038700
      *ISBAN-0002-I                                                     00038800
ID6797   05 PRO-INDEXADO.                                               00038900
ID6797     10 PRO-IND-INDEXADO       PIC X(1).                          00039000
ID6797     10 PRO-COEFICI-INDEX      PIC X(4).                          00039100
ID6797     10 PRO-COEFICI-VISUAL     PIC X(4).                          00039200
      *ISBAN-0002-F                                                     00039300
