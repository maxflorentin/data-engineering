         01  o20.
             03  TIPO-CUENTA               PIC  9(01).
             03  NRO-CUENTA                PIC  9(14).
             03  PAGOS-PERIODO                 PIC S9(13)V99 COMP-3.
             03  FECHA-ULT-PAGO            PIC  9(08).
             03  SUC-CUENTA                    PIC  9(03).
             03  CICLO-FACT                    PIC  9(02).
             03  LIMITE                        PIC S9(13)V99 COMP-3.
             03  FECHA-VTO-LIQ                 PIC  9(08).
             03  IMP-TOT-LIQ-AUS               PIC S9(13)V99 COMP-3.
             03  PAGO-MIN-LIQ                  PIC S9(13)V99 COMP-3.
             03  IMP-TOT-LIQ-DOL               PIC S9(13)V99 COMP-3.
             03  ADELANTOS-AUS                 PIC S9(13)V99 COMP-3.
             03  ADELANTOS-DOL                 PIC S9(13)V99 COMP-3.
             03  CONSUMOS-AUS                  PIC S9(13)V99 COMP-3.
             03  CONSUMOS-DOL                  PIC S9(13)V99 COMP-3.
             03  FEC-VTO-ORI                   PIC 9(08)     COMP-3.
             03  CPOST                         PIC X(04).
             03  MCA-VTO-ADIC                  PIC X(01).
             03  MARCA-INHAB-OC                PIC X(01).
             03  FEC-ULT-COMPRA                PIC 9(08)     COMP-3.
             03  AJUSTES-PERIODO-CR        PIC S9(13)V99 COMP-3.
             03  AJUSTES-PERIODO-DB        PIC S9(13)V99 COMP-3.
             03  FORMA-PAGO                    PIC  9(02).
             03  FECHA-PENULTIMO-PAGO          PIC  9(06).
             03  IMPORTE-ULTIMO-PAGO           PIC S9(13)V99 COMP-3.
             03  IMPORTE-PENULTIMO-PAGO        PIC S9(13)V99 COMP-3.
             03  SALDO-ACTUAL-AUS              PIC S9(13)V99 COMP-3.
             03  SALDO-ACTUAL-DOL              PIC S9(13)V99 COMP-3.
             03  FECHA-ULT-SALDO-ACTUAL        PIC  9(08).
             03  FECHA-LIQUIDACION             PIC  9(08).
             03  INICIO-MORA-NM            PIC  9(08).
             03  ULT-MES-MORA-NM           PIC  9(01).
             03  NO-EMISION-CARTA              PIC  X(01).
             03  CANT-CARTAS-INH               PIC  9(02).
             03  FECHA-ULT-CARTA               PIC  9(08).
             03  INDIC-ULT-CARTA-IGN           PIC  X(02).
             03  ULTIMA-CARTA-NM           PIC  9(02).
             03  TAB-CARTA OCCURS 10       PIC  X(01).
             03  PAGOS-PERIODO-DLRES           PIC S9(13)V99 COMP-3.
             03  FECHA-PENULT-PAGODOL          PIC  9(06).
             03  IMPORTE-ULT-PAGODOL           PIC S9(13)V99 COMP-3.
             03  IMPORTE-PENULT-PAGODOL        PIC S9(13)V99 COMP-3.
             03  AJU-PERIODO-CR-DLRES      PIC S9(13)V99 COMP-3.
             03  AJU-PERIODO-DB-DLRES      PIC S9(13)V99 COMP-3.
             03  PAGOS-TOTAL               PIC S9(13)V99 COMP-3.
             03  PAGO-MIN-IMP                  PIC S9(13)V99 COMP-3.
             03  DOLAR-VISA                PIC S9(11)V99 COMP-3.
             03  SALDO-UR-TOTAL            PIC S9(13)V99 COMP-3.
             03  PAGOS-CICLO-ANT           PIC S9(13)V99 COMP-3.
             03  PAGOS-DLRES-CICLO-ANT     PIC S9(13)V99 COMP-3.
             03  AJUSTES-CR-CICLO-ANT      PIC S9(13)V99 COMP-3.
             03  AJUSTES-DB-CICLO-ANT      PIC S9(13)V99 COMP-3.
             03  AJU-CR-DLRES-CICLO-ANT    PIC S9(13)V99 COMP-3.
             03  AJU-DB-DLRES-CICLO-ANT    PIC S9(13)V99 COMP-3.
             03  PAGOS-PERIODO-DLCONV          PIC S9(13)V99 COMP-3.
             03  PAGOS-DLCONV-CICLO-ANT        PIC S9(13)V99 COMP-3.
             03  AGMO-SUC-DES              PIC  999.
             03  AGMO-SUC-ORI              PIC  999.
             03  AGMO-TIPO-CTA             PIC  9.
             03  AGMO-NRO-CTA              PIC  9(15).
             03  AGMO-TIPO-PMO             PIC  9.
             03  AGMO-NRO-FIRM             PIC  99.
             03  FECHA-AGMO                    PIC  9(08).
             03  INDIC-AGMO                    PIC  X(01).
             03  TASA-NOMINAL-ANUAL            PIC  S9(3)V99 COMP-3.
             03  MARCA-BAJA-PLAN-CUOTAS        PIC  X(01).
             03  FECHA-PROX-VTO-LIQ            PIC  9(08).
             03  FECHA-PROX-LIQ                PIC  9(08).
             03  ESTADO-CUENTA-VISA            PIC  XX.
             03  TIPO-CUENTA-VISA              PIC  X.
             03  BLOQUEO-BLCO                  PIC  X.
             03  LETRA-COD                     PIC  X(2).
             03  TASA-NOMINAL-ANUAL-DOL        PIC  S9(3)V99 COMP-3.
             03  PAGOS-ACREDITAR-PESOS         PIC S9(09)V99 COMP-3.
             03  PAGOS-ACREDITAR-DOLAR         PIC S9(09)V99 COMP-3.
             03  MCA-CPHONE-MORA               PIC  X(01).
             03  GPO-GASTO                     PIC  9(01).
             03  GPO-TASA                      PIC  9(01).
             03  PZO-PG-MIN                    PIC  9(02).
             03  MCA-RET-RES                   PIC  9(01).
             03  LIM-LETRA                 PIC  X(01).
             03  LIM-COD                   PIC  9(01).
             03  FILLER                         PIC  X(02).
             03  AMPL-LIM-CPRA-IMP         PIC  9(13) COMP-3.
             03  AMPL-LIM-CPRA-VTO         PIC  9(08).
             03  LIMITE-CREDITO                PIC  9(13) COMP-3.
             03  MODEL-LIQ                     PIC  9(03).
             03  ESTADO-PTO-VTA                PIC  X(02).
             03  DIG-VERIF-CUENTA              PIC 9.
             03  DEU-NO-VEN-PESOS              PIC 9(12)V999.
             03  DEU-NO-VEN-DOL                PIC 9(10)V99.
             03  BCO-ORIGEN                    PIC X.
             03  TRATA-MORA-ESP                PIC X(01).
             03  EXCEP-IMP                     PIC 9(13) COMP-3.
             03  MK-ESTADO-CC-RT               PIC X.
             03  EXCEP-VTO                     PIC  9(08).
             03  EXCEP-TIPO                    PIC  X(01).
             03  MC-LIM-UNIF                   PIC  X(02).
             03  USUARIO-ALTA                  PIC X(8).
             03  MARCA-REESTRUC                PIC  X(02).
             03 PM1-IPG-30D                PIC S9(11)V99 COMP-3.
             03 PM1-IPG-60D                PIC S9(11)V99 COMP-3.
             03 PM1-IPG-90D                PIC S9(11)V99 COMP-3.
             03 PM1-IPG-120D               PIC S9(11)V99 COMP-3.
             03 PM1-IPG-150D               PIC S9(11)V99 COMP-3.
             03 PM1-IPG-180D               PIC S9(11)V99 COMP-3.
             03 PM2-IPG-30D                PIC S9(11)V99 COMP-3.
             03 PM2-IPG-60D                PIC S9(11)V99 COMP-3.
             03 PM2-IPG-90D                PIC S9(11)V99 COMP-3.
             03 PM2-IPG-120D               PIC S9(11)V99 COMP-3.
             03 PM2-IPG-150D               PIC S9(11)V99 COMP-3.
             03 PM2-IPG-180D               PIC S9(11)V99 COMP-3.
             03 FH-PM-IPG-30D              PIC 9(08) COMP-3.
             03 FH-PM-IPG-60D              PIC 9(08) COMP-3.
             03 FH-PM-IPG-90D              PIC 9(08) COMP-3.
             03 FH-PM-IPG-120D             PIC 9(08) COMP-3.
             03 FH-PM-IPG-150D             PIC 9(08) COMP-3.
             03 FH-PM-IPG-180D             PIC 9(08) COMP-3.
             03  FILLER                             PIC X(368).
