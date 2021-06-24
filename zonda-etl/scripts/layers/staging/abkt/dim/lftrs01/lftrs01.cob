          01  TS1.
               10  LOCATION                 PIC X(06).
               10  GROUP-PREFIX         	PIC X.
               10  GROUP-NUMBER         	PIC 9(06).
               10  RECORD-TYPE              PIC X(02).
               10  SEQUENCE                 PIC 9(03).
               10  FILLER-1                 PIC X(17).
               10  CURR-CODE                PIC X(03).
               10  EXCH-RATE-DISP           PIC S9(6)V9(9) COMP-3.
               10  EXCH-RATE-IND            PIC X.
               10  PY-AMT                   PIC S9(11)V99 COMP-3.
               10  PY-AMT-BASE-CURR         PIC S9(11)V99 COMP-3.
               10  PY-TOTAL-DEBIT           PIC S9(11)V99 COMP-3.
               10  PY-TOT-DEBIT-AMT         PIC S9(11)V99 COMP-3.
               10  PY-TOT-DEBIT-CURR        PIC S9(11)V99 COMP-3.
               10  PY-TOT-CHGES-DEBIT       PIC S9(11)V99 COMP-3.
               10  PY-DEBIT-ACCT-NO         PIC X(15).
               10  PY-DEBIT-MNEM            PIC X(5).
               10  PY-DEBIT-CURR-CODE       PIC X(3).
               10  PY-DEBIT-RATE-DISP       PIC S9(6)V9(9) COMP-3.
               10  PY-DEBIT-RATE-IND        PIC X.
               10  PY-TOTAL-CREDIT          PIC S9(11)V99 COMP-3.
               10  PY-TOT-CR-AMT            PIC S9(11)V99 COMP-3.
               10  PY-TOT-CR-CURR           PIC S9(11)V99 COMP-3.
               10  PY-TOT-CHGES-CREDIT      PIC S9(11)V99 COMP-3.
               10  PY-CR-ACCT-NO            PIC X(15).
               10  PY-CR-MNEM               PIC X(5).
               10  PY-CR-CURR-CODE          PIC X(3).
               10  PY-CR-RATE-DISP          PIC S9(6)V9(9) COMP-3.
               10  DOC1-NAME                PIC X(8).
               10  DOC1-CAS                 PIC X.
               10  DOC2-NAME                PIC X(8).
               10  DOC2-CAS                 PIC X.
               10  DOC3-NAME                PIC X(8).
               10  DOC3-CAS                 PIC X.
               10  CHARGE1-MNEM             PIC X(5).
               10  CHARGE1-AMT              PIC S9(11)V99 COMP-3.
               10  CHARGE1-CURR             PIC XXX.
               10  CHARGE1-RATE-DISP        PIC S9(6)V9(9) COMP-3.
               10  CHARGE1-TAKE             PIC X.
               10  CHARGE1-RATE-IND         PIC X.
               10  CHARGE2-MNEM             PIC X(5).
               10  CHARGE2-AMT              PIC S9(11)V99 COMP-3.
               10  CHARGE2-CURR             PIC XXX.
               10  CHARGE2-RATE-DISP        PIC S9(6)V9(9) COMP-3.
               10  CHARGE2-TAKE             PIC X.
               10  CHARGE2-RATE-IND         PIC X.
               10  CHARGE3-MNEM             PIC X(5).
               10  CHARGE3-AMT              PIC S9(11)V99 COMP-3.
               10  CHARGE3-CURR             PIC XXX.
               10  CHARGE3-RATE-DISP        PIC S9(6)V9(9) COMP-3.
               10  CHARGE3-TAKE             PIC X.
               10  CHARGE3-RATE-IND         PIC X.
               10  CHARGE4-MNEM             PIC X(5).
               10  CHARGE4-AMT              PIC S9(11)V99 COMP-3.
               10  CHARGE4-CURR             PIC XXX.
               10  CHARGE4-RATE-DISP        PIC S9(6)V9(9) COMP-3.
               10  CHARGE4-TAKE             PIC X.
               10  CHARGE4-RATE-IND         PIC X.
               10  COMM-MNEM                PIC X(5).
               10  COMM-AMT                 PIC S9(11)V99 COMP-3.
               10  COMM-CURR                PIC XXX.
               10  COMM-RATE-DISP           PIC S9(6)V9(9) COMP-3.
               10  COMM-TAKE                PIC X.
               10  COMM-RATE-IND            PIC X.
               10  RELEASE-DATE1            PIC S9(7) COMP-3.
               10  RELEASE-DATE2            PIC S9(7) COMP-3.
               10  RELEASE-TIME1            PIC S9(6) COMP-3.
               10  RELEASE-TIME2            PIC S9(6) COMP-3.
               10  RELEASE-CODE1            PIC S9(3) COMP-3.
               10  RELEASE-CODE2            PIC S9(3) COMP-3.
               10  AUTH1                    PIC X(04).
               10  AUTH2                    PIC X(04).
               10  PYMT-HOLD-DATE           PIC S9(7) COMP-3.
               10  LAST-PYMT-DATE           PIC S9(7) COMP-3.
               10  PY-VALUE-DATE            PIC S9(7) COMP-3.
               10  PY-CREATE-DATE           PIC S9(7) COMP-3.
               10  PY-CREATE-TIME           PIC S9(6) COMP-3.
               10  PRINT-DRAFT-LTR-IND      PIC X.
               10  TAX-CRG1-MNEM            PIC X(5).
               10  TAX-CRG1-AMT             PIC S9(11)V99 COMP-3.
               10  TAX-CRG1-AMT-DR-CUR      PIC S9(11)V99 COMP-3.
               10  TAX-CRG2-MNEM            PIC X(5).
               10  TAX-CRG2-AMT             PIC S9(11)V99 COMP-3.
               10  TAX-CRG2-AMT-DR-CUR      PIC S9(11)V99 COMP-3.
               10  TAX-CRG3-MNEM            PIC X(5).
               10  TAX-CRG3-AMT             PIC S9(11)V99 COMP-3.
               10  TAX-CRG3-AMT-DR-CUR      PIC S9(11)V99 COMP-3.
               10  TAX-CRG4-MNEM            PIC X(5).
               10  TAX-CRG4-AMT             PIC S9(11)V99 COMP-3.
               10  TAX-CRG4-AMT-DR-CUR      PIC S9(11)V99 COMP-3.
               10  TAX-COMM-MNEM            PIC X(5).
               10  TAX-COMM-AMT             PIC S9(11)V99 COMP-3.
               10  TAX-COMM-AMT-DR-CUR      PIC S9(11)V99 COMP-3.
               10  CHARGE1-AMT-DR-CUR       PIC S9(11)V99 COMP-3.
               10  CHARGE2-AMT-DR-CUR       PIC S9(11)V99 COMP-3.
               10  CHARGE3-AMT-DR-CUR       PIC S9(11)V99 COMP-3.
               10  CHARGE4-AMT-DR-CUR       PIC S9(11)V99 COMP-3.
               10  COMM-AMT-DR-CUR          PIC S9(11)V99 COMP-3.
               10  PRODUCT-CODE             PIC X(03).
               10  PY-CONTRACT              PIC X(15).
               10  PY-DEBIT-CHARGES-ACCT    PIC X(15).
               10  PY-DEBIT-CHARGES-MNEM    PIC X(5).
               10  PY-DEBIT-CHARGES-CUR     PIC X(3).
               10  PY-DEBIT-CHGES-RATE-DISP PIC S9(6)V9(9) COMP-3.
               10  PY-DEBIT-CHGES-RATE-IND  PIC X.
               10  PY-AMT-ORIG              PIC S9(11)V99 COMP-3.
               10  OPER-CODE                PIC X(4).
               10  INSTR-CODE               PIC X(4).
               10  INSTR-TEXT               PIC X(30).
               10  TRANS-TYPE-CODE          PIC X(3).
               10  RINST-ACCT               PIC X(15).
               10  RINST-CITY               PIC X(04).
               10  RINST-ACRN               PIC X(08).
               10  RINST-SW-ID              PIC X(12).
               10  RINST-CNTRY              PIC X(05).
               10  RINST-NADD               OCCURS 4 TIMES PIC X(35).
               10  RINST-BIC-TYPE           PIC X(2).
               10  RINST-ROUTE-NUMBER       PIC X(34).
               10  REGUL-REPORTING          OCCURS 3 TIMES PIC X(35).
               10  INSTRUCTED-CURR-CODE     PIC X(3).
               10  INSTRUCTED-AMT           PIC S9(11)V99 COMP-3.
               10  INSTR-EXCHANGE-RATE      PIC S9(6)V9(9) COMP-3.
               10  SEND-CHGES-CURR-CODE     PIC X(3).
               10  SEND-CHGES-AMT           PIC S9(11)V99 COMP-3.
               10  RECV-CHGES-CURR-CODE     PIC X(3).
               10  RECV-CHGES-AMT           PIC S9(11)V99 COMP-3.
               10  SWIFT-MSG-NO             PIC X(8).
               10  13C-DETAILS              PIC X(19).
               10  ADDIT-INST               OCCURS 12 TIMES.
                   15  ADDIT-INST-CODE      PIC X(04).
                   15  ADDIT-INST-TEXT      PIC X(30).
               10  ADDIT-INSTR-IND          PIC X.
               10  ADDIT-SEND-IND           PIC X.
               10  ADDIT-SEND-CHARG         OCCURS 5 TIMES.
                   15  ADDIT-SEND-CURR      PIC X(03).
                   15  ADDIT-SEND-AMT       PIC S9(11)V99 COMP-3.
               10  TAX1-COMM-MNEM           PIC X(5).
               10  TAX1-COMM-AMT            PIC S9(11)V99 COMP-3.
               10  TAX1-COMM-AMT-DR-CUR     PIC S9(11)V99 COMP-3.
               10  TAX2-COMM-MNEM           PIC X(5).
               10  TAX2-COMM-AMT            PIC S9(11)V99 COMP-3.
               10  TAX2-COMM-AMT-DR-CUR     PIC S9(11)V99 COMP-3.
               10  TAX3-COMM-MNEM           PIC X(5).
               10  TAX3-COMM-AMT            PIC S9(11)V99 COMP-3.
               10  TAX3-COMM-AMT-DR-CUR     PIC S9(11)V99 COMP-3.
               10  TAX4-COMM-MNEM           PIC X(5).
               10  TAX4-COMM-AMT            PIC S9(11)V99 COMP-3.
               10  TAX4-COMM-AMT-DR-CUR     PIC S9(11)V99 COMP-3.
               10  TAX5-COMM-MNEM           PIC X(5).
               10  TAX5-COMM-AMT            PIC S9(11)V99 COMP-3.
               10  TAX5-COMM-AMT-DR-CUR     PIC S9(11)V99 COMP-3.
               10  FILLER-2                 PIC X(220).