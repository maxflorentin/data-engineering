      ******************************************************************
      *                TRANSACCIONES BANELCO                           *
      ******************************************************************
      * 03-05-2004 COPY DEL TLF QUE RESUME LOS CAMPOS DE LOS COPYS
      *            :WABAETLF: Y SEMSG.
      *            CONSERVA LOS NOMBRES DE CAMPOS DE LOS COPYS
      ******************************************************************
      * ISBAN-0001 SE MODIFICA LA COPY PARA QUE INCLUYA EL TIPO DE
      *            EXTRACCION
      ******************************************************************
      * ISBAN-0002 SE MODIFICA LA COPY PARA QUE INCLUYA LAS TRANSACCION_
      *            ES DEBIN
      ******************************************************************
      * DEBIN-0002 SE MODIFICA LA COPY PARA LA DEFINICION DE LOS 1REOS
      *            CAMPOS DEL TLF - AGREGADO DEFINICION PRODUCT-IND
      *            :WABAETLF:-PRODUCT-IND = '00' TLF
      *            :WABAETLF:-PRODUCT-IND = '04' PTLF
      ******************************************************************
      * DEBIN-0003 SE MODIFICA LAS POSICIONES PARA DEBIN CON EL TLF
      *            BINARIO
      ******************************************************************

      *  TIPO       : SAM
      *  LONG REG.  : 600

       01  :WABAETLF:-SEM-REGISTRO.
      *
      *    REGISTRO DE DETALLE
      *
           02  :WABAETLF:-SEM-REGISTRO-DETALLE.
      *DEBIN-0002-I.
               03  :WABAETLF:-ROUTE.
                  04 :WABAETLF:-STAT                PIC 9(02).
                  04  :WABAETLF:-PATH.
                   05  :WABAETLF:-PRODUCT-IND       PIC X(02).
                   05  :WABAETLF:-RELEASE-NUMBER    PIC X(02).
                   05  :WABAETLF:-DPC-NUMBER        PIC 9(04).
                   05  FILLER1                      PIC X(04). --DIF
      *        SORTEOS: DE QUIEN ES. BNLC=BANELCO                             0
      *DEBIN-0002-F.
               05  :WABAETLF:-ORIGEN-SORTEO REDEFINES FILLER1.
      *        1ER USO DE LA TARJETA REIMPRESA.                               0
                   06  :WABAETLF:-1ER-USO       PIC X(01).
                       88  :WABAETLF:-1ER-USO-SI   VALUE 'P'.
                   06  FILLER                   PIC X(03).

               03  FILLER                       PIC X(10).
               03  :WABAETLF:-SEMSGTYP          PIC 9(04).
      *                                                           25-28       0
               03  FILLER                       PIC X(18).
               03  :WABAETLF:-SEMCACCT          PIC X(19).
      *                                                           47-65       0
               03  :WABAETLF:-SEMTCODE          PIC 99.
      *        PARA VER VALORES DE SEMTCODE (TIPO DE OPERACION)               0
      *        VER Y UTILIZAR RUTINA ABAE800                                  00

               03  :WABAETLF:-SEMTFACC          PIC 99.
               03  :WABAETLF:-SEMTTACC          PIC 99.
      *            TIPOS DE CUENTA FROM O TO                                  00
      *             PARA CONVERTIR A TIPOS BANCO RIO                          00
      *             UTILIZAR RUTINA ZO9C1500                                  00
      *            01 = CUENTA CORRIENTE EN PESOS.
      *            02 = CUENTA CORRIENTE EN DLARES.
      *            11 = CAJA DE AHORROS EN PESOS.
      *            12 = CAJA DE AHORROS EN DLARES.
      *            13 = CUENTA ESPECIAL.
      *            31 = CUENTA DE CRDITO EN PESOS.
      *            32 = CUENTA DE CRDITO EN DLARES.
      *            40 = NMERO DE TARJETA PARA PAGO CON TARJETA BANELCO.
      *            50 = CUENTA DE CAPITALIZACIN (AFJP).
      *            60 = CUENTA DE INVERSOR PARA PF.
      *            70 = CUENTA DE FONDO DE INVERSION.

               03  FILLER                       PIC X(01).
               03  :WABAETLF:-SEMAMT1           PIC S9(14)V99  COMP.
               03  :WABAETLF:-SEMAMT2           PIC S9(14)V99  COMP.
               03  :WABAETLF:-SEMAMT3           PIC S9(14)V99  COMP.

               03  :WABAETLF:-FIID              PIC X(04).
      *              COD.INSTIT. P/TARJ.CR                        97-100      0
               03  :WABAETLF:-AUTH-CODE         PIC 9(06).
      *              COD.AUTORIZACION                            101-106      0
               03  :WABAETLF:-SYS-ID            PIC X(01).
      *              COD.IDENTIF.TARJ.CR                         107-107      0

               03  :WABAETLF:-TRACE-NO          PIC 9(06).
      *              NUM.P/HOST P/IDENT.TRANS.                   108-113      0

               03  :WABAETLF:-SEMTRDAT          PIC 9(04).
      *            INICIO TRANSACCION (MMDD)                                  0

               03  :WABAETLF:-SEMPDAT           PIC 9(04).
      *            BUSINESS DAY (MMDD)                                        0

               03  FILLER                       PIC X(04).
               03  :WABAETLF:-SEMTRTIM          PIC 9(06).
      *            INICIO TRANSACCION (HHMMSS)                                0

               03  :WABAETLF:-SEMCOD-TERM       PIC X(04).
               03  :WABAETLF:-SEMCOD-TARJ       PIC X(04).
               03  :WABAETLF:-SEMCCODE          PIC X(03).
      *            MONEDA DE COMPENSACION                                     0
      *            032 = PESOS.
      *            840 = DLARES.
                                                                              0
               03  :WABAETLF:-SEMRREV           PIC 99.
      *            CODIGO DE REVERSO                                          0
      *            00    NO APLICABLE  (SIN ERROR)                            00
      *            01    TIME-OUT                                             00
      *            02    RESPUESTA INVALIDA SYSTEM ERROR                      00
      *            03    DESTINO NO DISPONIBLE                                00
      *            08    ANULACION TRANSAC. POR EL CLIENTE                    00
      *            10    ERROR DE HARDWARE (ATM)                              00
      *            20    TRANSACCION SOSPECHOSA                               00

               03  FILLER                       PIC X(06).
               03  :WABAETLF:-ID-CANAL          PIC X(03).
               03  :WABAETLF:-MARCA-IVA         PIC X(01).
      *        0 = NO PERTENECE A CATEGORA CONSUMIDOR FINAL.
      *        1 = PERTENECE A CATEGORA CONSUMIDOR FINAL.
      *
               03  FILLER                       PIC X(01).
               03  :WABAETLF:-RESP-COD.
                   05  :WABAETLF:-SEMRCARD      PIC X.
                   05  :WABAETLF:-SEMTRNAD      PIC XX.
      *        UTILIZAR RUTINA ABAE801                  .       157-158       00
      *        PARA DETERMINAR SI ES RECHAZO Y MENSAJE ASOCIADO               00

               03  :WABAETLF:-SEMCITY           PIC X(15).
               03  :WABAETLF:-ORIGL             PIC X(20).
               03  :WABAETLF:-TRANSF-BCOS2      REDEFINES
                                                :WABAETLF:-ORIGL.
                   05  :WABAETLF:-T72-DNI-CUIL  PIC X(12).
                   05  :WABAETLF:-T72-COMISION  PIC S9(4)V99.
                   05  FILLER                   PIC X(02).

      * INFORMACION DEL SEGURO
               03  :WABAETLF:-SEGURO        REDEFINES :WABAETLF:-ORIGL. L.
                05 :WABAETLF:-SEG-TIPO-MOV        PIC X(1).
                05 :WABAETLF:-SEG-TIPO-SEGURO     PIC X(1).
                05 :WABAETLF:-SEG-IDENTIFICACION  PIC X(1).
                05 :WABAETLF:-SEG-COSTO-COBERTURA PIC 9(4)V9(2).
                05 FILLER                         PIC X(11).

      * MARCAS DE SORTEOS
               03  :WABAETLF:-PAGOS-SORTEOS REDEFINES :WABAETLF:-ORIGL. L.
                05 :WABAETLF:-SOR-BANELCO         PIC X(1).
                 88  :WABAETLF:-SOR-BANELCO-NO-VIG      VALUE SPACES.
                 88  :WABAETLF:-SOR-BANELCO-PART        VALUE 'P'.
                 88  :WABAETLF:-SOR-BANELCO-NO-PAR      VALUE 'N'.
                 88  :WABAETLF:-SOR-BANELCO-NO-EMPR     VALUE 'E'.
                 88  :WABAETLF:-SOR-BANELCO-GANO        VALUE 'G'.
                05 :WABAETLF:-SOR-BANELCO-IMPORTE PIC 9(05)V99.
                05 :WABAETLF:-SOR-BANCOS          PIC X(1).
                 88  :WABAETLF:-SOR-BANCOS-NO-VIG      VALUE SPACES.
                 88  :WABAETLF:-SOR-BANCOS-PART        VALUE 'P'.
                 88  :WABAETLF:-SOR-BANCOS-NO-PART     VALUE 'N'.
                 88  :WABAETLF:-SOR-BANCOS-NO-EMPR     VALUE 'E'.
                 88  :WABAETLF:-SOR-BANCOS-GANO        VALUE 'G'.
                05 :WABAETLF:-SOR-BANCOS-IMPORTE  PIC 9(05)V99.
                05 FILLER                         PIC X(4).
      *
               03  FILLER                       PIC X(22).
      *                                                                       0
      *            NRO DE CTA CONSULTA O DEBITO 218-236                       0
               03  :WABAETLF:-SEMFALEN          PIC X(02).
               03  :WABAETLF:-SEMFANUM          PIC X(19).
               03  :WABAETLF:-SEMFANUMX REDEFINES :WABAETLF:-SEMFANUM.
                   05  :WABAETLF:-SEMFANUM-DEB  PIC 9(12).
                   05  FILLER                   PIC X(07).
               03  :WABAETLF:-SEMFANUMX1 REDEFINES :WABAETLF:-SEMFANUM.
                   05  :WABAETLF:-SEMFANUM-CRED PIC 9(16).
                   05  FILLER                   PIC X(03).
               03  :WABAETLF:-SEMFANUMX2 REDEFINES :WABAETLF:-SEMFANUM.
                   05  :WABAETLF:-SEMFANUM-SUC  PIC 9(03).
                   05  FILLER                   PIC X(02).
                   05  :WABAETLF:-SEMFANUM-CTA  PIC 9(07).
                   05  FILLER                   PIC X(07).

      *                                                          239-257      0
      *            IDEM ANTERIOR AFECTADA POR CREDITO                         0
               03  :WABAETLF:-SEMTALEN          PIC X(02).
               03  :WABAETLF:-SEMTANUM          PIC X(19).
               03  :WABAETLF:-SEMTANUMX REDEFINES :WABAETLF:-SEMTANUM.
                   05  :WABAETLF:-SEMTANUM-DEB  PIC 9(12).
                   05  FILLER                   PIC X(07).
               03  :WABAETLF:-SEMTANUMX1 REDEFINES :WABAETLF:-SEMTANUM.
                   05  :WABAETLF:-SEMTANUM-CRED PIC 9(16).
                   05  FILLER                   PIC X(03).
               03  :WABAETLF:-SEMTANUMX2 REDEFINES :WABAETLF:-SEMTANUM.
                   05  :WABAETLF:-SEMTANUM-SUC  PIC 9(03).
                   05  FILLER                   PIC X(02).
                   05  :WABAETLF:-SEMTANUM-CTA  PIC 9(07).
                   05  FILLER                   PIC X(07).

130603         03  FILLER                       PIC X(28).
      *                                                          258-285
130603         03  :WABAETLF:-SEMTERM-COUNTRY   PIC X(02).
      *                                                          286-287
130603         03  FILLER                       PIC X(16).
      *                                                          288-303
               03  :WABAETLF:-SEMBANC           PIC X(04).
      *                                                          304-307

               03  :WABAETLF:-SEMFIID           PIC X(04).
      *            INSTITUCION A LA QUE PERTENECE EL CAJERO      308-311

               03  :WABAETLF:-SEMTNLOC          PIC X(25).

               03  :WABAETLF:-TRANS-COELSA      REDEFINES
                                                :WABAETLF:-SEMTNLOC.
                 05  :WABAETLF:-CBU-DESTINO     PIC X(22).
                 05  :WABAETLF:-TITULARIDAD     PIC X(01).
                   88  :WABAETLF:-MISMO-TITULAR    VALUE '1' '3'.
                 05  FILLER                     PIC X(02).

               03  :WABAETLF:-SEMTCOMER         PIC X(25).
      *            INSTITUCION A LA QUE PERTENECE EL CAJERO
      *                                                                       0
               03  :WABAETLF:-SEMTATMI          PIC X(16).
      *            CODIGO IDENTIFICATORIO DE LA TERMINAL         362-377      0

               03  FILLER                       PIC X(04).
      *                                                          378-381      0
      *        03  :WABAETLF:-SEMCLASE          PIC X(01).
      *            CLASE DE TARJETA SOLA PARA BANCO TORNQUIST    382-382      0
      *            VA A VENIR CUANDO LA USE POR PRIMERA VEZ                   0
      *                                                                       0
               03  FILLER                       PIC X(04).
      *                                                          382-385      0
               03  :WABAETLF:-SEMTRENU          PIC 9(06).
      *            NUMERO DE RECIBO ATM                                       0

               03  FILLER                       PIC X(08).
               03  :WABAETLF:-SEMBANC-1         PIC X(04).
               03  FILLER                       PIC X(25).
               03  :WABAETLF:-IMP-BASE-SORTEO   PIC S9(16)V99   COMP.
      *            SORTEOS: IMPORTE DE LA OPERACION ORIGINAL
      *ISBAN-0001-I
      *        03  FILLER                       PIC X(10).
               03  FILLER                       PIC X(09).
               03 :WABAETLF:-TIPO-EXTR          PIC X(01).
      *ISBAN-0001-I
               03  :WABAETLF:-SEMTIDEP          PIC X.
      *            TIPO DE DEPOSITO                              447-447

               03  :WABAETLF:-SEMTIDEB          PIC X.
      *            TIPO DE DEBITO                                448-448

               03  :WABAETLF:-SEMTIPAG          PIC X.
      *            TIPO DE PAGO                                  449-449

               03  :WABAETLF:-SEMTITAR          PIC X.
      *            TIPO DE TARJETA                               450-450

130603*        03  FILLER                       PIC X(43).
      *DEBIN-0003-I.
130603         03  FILLER                       PIC X(03).
      *..............................................................
      *                       D E B I N
      *..............................................................
               03 :WABAETLF:-DEBIN-NOMBRE-ORIG.
                 05 :WABAETLF:-NOMBRE-ORIGEN    PIC X(18).
                 05 :WABAETLF:-TIP-DEBIN        PIC X(02).
                 05 FILLER                      PIC X(17).
               03 FILLER                        PIC X(03).
      *..............................................................
130603         03  :WABAETLF:-T3DATA            PIC X(107).
               03  :WABAETLF:-DEBIN-BCOS1 REDEFINES
                   :WABAETLF:-T3DATA.
                 05 :WABAETLF:-DEBIN.
                   10 :WABAETLF:-ID-DEBIN       PIC X(22).
                   10 :WABAETLF:-CUIT-BCO-CDOR  PIC X(12).
                   10 :WABAETLF:-CBU-BCO-CDOR   PIC X(22).
                   10 :WABAETLF:-CUIT-BCO-VDOR  PIC X(12).
                   10 :WABAETLF:-CBU-BCO-VDOR   PIC X(22).
                   10 :WABAETLF:-FEC-NEG-COEL   PIC X(08).
                   10 :WABAETLF:-SCORING-DEBIN  PIC 9(03).
                   10 :WABAETLF:-CPTO-DEBIN     PIC 9(01).
                   10 :WABAETLF:-DESC-CPTO-DEB  PIC X(03).
                   10 :WABAETLF:-CORRESP-TITU   PIC X(01).
                   10 FILLER                    PIC X(01).
      *DEBIN-0003-F.
      *..............................................................
      *                                                          451-495
               03  :WABAETLF:-RESTO        REDEFINES
                   :WABAETLF:-T3DATA.
                 05 :WABAETLF:-SIGUE-REGISTRO.
                 10  FILLER                      PIC X(2).
      *..............................................................
      *                                                          451-495
080604           10  :WABAETLF:-MONEDA-ORIGEN     PIC X(3).
      *            CODIGO DE MONEDA DE LA OPERACION ORIGINAL     496-498
130603           10  :WABAETLF:-SEMIMPO-ORIGINAL  PIC S9(16)V99 COMP.
      *                                                          499-506
                 10  :WABAETLF:-SEMFEFA           PIC X(006).
      *                                                          507-512      0
                 10  :WABAETLF:-SEMCOTCO          PIC 9(14)V99 COMP.
      *                                                          513-520      0
                 10  :WABAETLF:-SEMCOTVE          PIC 9(14)V99 COMP.
      *                                                          521-528      0
                 10  :WABAETLF:-SEMAMT5           PIC 9(14)V99 COMP.
      *                                                          529-536      0
150903           10  :WABAETLF:-NRO-CONTROL       PIC X(04).
      *            NRO DE CONTROL EN P.E.S.                      537-540      0

130603           10  FILLER                       PIC X(19).
      *                                                          541-559
               10  :WABAETLF:-INF-ADIC-TRANS.
                15 :WABAETLF:-TRANS-BANCO-RECEPTOR        PIC X(04).
                15 :WABAETLF:-TRANS-CODIGO-CONCEPTO       PIC X(01).
                15 :WABAETLF:-TRANS-DESC-CONCEPTO         PIC X(03).
                15 :WABAETLF:-TRANS-REFERENCIA            PIC X(12).
                15 FILLER                                 PIC X(01).
      *
130603         10  :WABAETLF:-SEMNRO-CARGO      PIC X(08).
      *                                                          581-588
CM0305         10  :WABAETLF:-PORC-DEVL-CLTE    PIC X(03).
CM0305         10  :WABAETLF:-PORC-DEVL-CLTE-NRO REDEFINES
CM0305             :WABAETLF:-PORC-DEVL-CLTE    PIC 9(02)V9.
      *                                                          589-591
CM0305         10  :WABAETLF:-PORC-DEVL-COMER   PIC X(03).
CM0305         10  :WABAETLF:-PORC-DEVL-COMER-NRO REDEFINES
CM0305             :WABAETLF:-PORC-DEVL-COMER   PIC 9(02)V9.
      *                                                          592-594
CM0305         10  :WABAETLF:-RUBRO             PIC X(04).
      *            RUBRO DE LA COMPRA                            595-598
CM0305         10  :WABAETLF:-BALANCEO          PIC X(01).
      *            INDICA SI EL BALANCEO SE EFECTUO ANTES O
      *            DESPUES DE LA OPERACION
               10  :WABAETLF:-TIPO-CAJERO       PIC X(01).
                 88 :WABAETLF:-TIPO-CAJERO-VALIDO VALUE 'N' 'D' 'C' 'A'.
      *            N = NEUTRAL
      *            D = LOBBY COMPARTIDO
      *            C = PROPIO
      *            A = BANELCO
      *-----------------------------------------------------------------
      *    REGISTRO HEADER
      *
       01  :WABAETLF:-SEM-REGISTRO-HEADER.
               03  :WABAETLF:-SEMTYP-HEADER     PIC X(02).
      *            'FH'    IDENTIFICADOR DEL HEADER
               03  :WABAETLF:-SEMLTH-HEADER     PIC 9(04).
      *            0038    LONGITUD DE ESTE REGISTRO
               03  :WABAETLF:-SEMFIL-HEADER     PIC X(08).
      *            IDENTIFICADOR DEL ARCHIVO EXTRAIDO
      *            'TLFALL  ' TLF DE TRANSACCIONES COMPLETO
      *            'TLFXXXX ' TLF DE LAS INSTIT. DEL GRUPO REFRESH
      *                       EXPRESADO EN XXXX
      *            'REJXXXX ' PAGOS RECHAZADOS DE LAS INST. DEL GRUPO
      *                       REFRESH EXPRESADO EN XXXX
      *            'SAFYYY  ' SAF DE OP. QUE PROCESA EL DPC INDICADO EN
      *                       YYY
               03  :WABAETLF:-SEMCDTE-HEADER    PIC 9(06).
      *            FECHA DE PROCESO EN QUE SE CREO LA CINTA (AAMMDD)
               03  :WABAETLF:-SEMCTIM-HEADER    PIC 9(06).
      *            HORA  DE PROCESO EN QUE SE CREO LA CINTA (HHMMSS)
               03  :WABAETLF:-SEMCTL-HEADER     PIC 9(06).
      *            FECHA DEL DIA DE NEGOCIO (AAMMDD)
               03  :WABAETLF:-SEM-LOGICAL-NET   PIC X(04).
      *            IDENTIFICACION DE LA RED
      *            'PRO1' RED DE PRODUCCION
      *            'TES1' RED DE PRUEBAS
               03  :WABAETLF:-SEM-RELEASE-NUM   PIC 9(02).
      *            30 RELEASE AL QUE CORRESPONDE EL FORMATO DE REG
               03  FILLER                       PIC X(562).
      *                                                          600-600
      *    REGISTRO TRAILER
      *
       01  :WABAETLF:-SEM-REGISTRO-TRAILER.                             ER.
               03  :WABAETLF:-SEMTYP-TRAILER    PIC X(02).
      *            'FH'    IDENTIFICADOR DEL HEADER
               03  :WABAETLF:-SEMLTH-TRAILER    PIC 9(04).
      *            0038    LONGITUD DE ESTE REGISTRO
               03  :WABAETLF:-SEMFIL-TRAILER    PIC X(08).
      *            IDENTIFICADOR DEL ARCHIVO EXTRAIDO
      *            'TLFALL  ' TLF DE TRANSACCIONES COMPLETO
      *            'TLFXXXX ' TLF DE LAS INSTIT. DEL GRUPO REFRESH
      *                       EXPRESADO EN XXXX
      *            'REJXXXX ' PAGOS RECHAZADOS DE LAS INST. DEL GRUPO
      *                       REFRESH EXPRESADO EN XXXX
      *            'SAFYYY  ' SAF DE OP. QUE PROCESA EL DPC INDICADO EN
      *                       YYY
               03  :WABAETLF:-SEMCDTE-TRAILER   PIC 9(06).
      *            FECHA DE PROCESO EN QUE SE CREO LA CINTA (AAMMDD)
               03  :WABAETLF:-SEMCTIM-TRAILER   PIC 9(06).
      *            HORA  DE PROCESO EN QUE SE CREO LA CINTA (HHMMSS)
               03  :WABAETLF:-SEMTRC1-TRAILER   PIC 9(06).
      *            CANTIDAD DE REGISTROS EXTRAIDOS EXCLUIDOS
      *            HEADERS Y TRAILERS
               03  :WABAETLF:-SEMTRC2-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMTRC3-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMTRC4-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMTRC5-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMTRC6-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMTRC9-TRAILER   PIC 9(06).
      *            USO FUTURO
               03  :WABAETLF:-SEMAMT-TYP1       PIC 9(11)V99.
      *            SUMATORIA DEL CAMPO AMT-1 DE TODOS LOS REGISTROS
      *            EXTRAIDOS
               03  FILLER                       PIC X(519).

      ******************************************************************
      ******************************************************************
      ******************************************************************