           02  DRV.
               10  LENGTH             PIC S9(08) BINARY.
               10  TENANT-ID          PIC S9(08) BINARY.
               10  CUSTOMER-ID        PIC  X(20).
               10  REC-ID             PIC  X(02).
               10  REC-ID-NUM         PIC  9(02).
               10  REC-SEQ-NUM        PIC  9(02).
               10  ACCOUNT-ID                 PIC  X(20).
               10  PREV-ACCT-ID               PIC  X(20).
               10  DATE-OPEN                  PIC  9(08)  BINARY.
               10  DATE-CLOSED                PIC  9(08)  BINARY.
               10  REASON-CLOSED              PIC  9(01).
               10  NUM-PARTIES                PIC  9(01).
               10  DIVISOR OCCURS 2 TIMES     PIC  9(02)V9(02)
                                                          BINARY.
               10  LIABILITY-IND              PIC  9(01).
               10  PRIMARY-ACCT-IND           PIC  9(01).
               10  CASH-ADV-IND               PIC  9(01).
               10  ACCOUNT-TYPE               PIC S9(03)  BINARY.
               10  ACCOUNT-SPR-TYPE           PIC  9(01).
               10  DATE-FIRST-ACTIVE          PIC  9(08)  BINARY.
               10  DATE-LAST-MERCH            PIC  9(08)  BINARY.
               10  DATE-LAST-PAY              PIC  9(08)  BINARY.
               10  DATE-LAST-CASH-ADV         PIC  9(08)  BINARY.
               10  DATE-START-OVERLIMIT       PIC  9(08)  BINARY.
               10  DATE-START-DELQ            PIC  9(08)  BINARY.
               10  CURR-BALANCE               PIC S9(09)  BINARY.
               10  CURR-LIMIT                 PIC S9(09)  BINARY.
               10  CURR-CYCLES-DELQ           PIC  9(01).
               10  PAYMENT-METHOD             PIC  9(01).
               10  FACILITY-1                 PIC  9(01).
               10  FACILITY-2                 PIC  9(01).
               10  FACILITY-3                 PIC  9(01).
               10  FACILITY-4                 PIC  9(01).
               10  FACILITY-RENEWAL-IND       PIC  9(01).
               10  TRIAD-CAT                  PIC  9(02).
               10  SHADOW-LIMIT               PIC S9(09)  BINARY.
               10  CASH-LIMIT   OCCURS 2 TIMES
                                                 PIC S9(09)  BINARY.
               10  VAL-FEES-GROUP.
                   15  VAL-FEES OCCURS 5 TIMES
                                                 PIC S9(09)  BINARY.
               10  CREDIT-INTEREST            PIC S9(09)  BINARY.
               10  DECL-TXN1-NUM              PIC S9(03)  BINARY.
               10  DECL-TXN1-VAL              PIC S9(09)  BINARY.
               10  DECL-TXN2-NUM              PIC S9(03)  BINARY.
               10  DECL-TXN2-VAL              PIC S9(09)  BINARY.
               10  APR-TXN-SHAD-NUM           PIC S9(03)  BINARY.
               10  APR-TXN-SHAD-VAL           PIC S9(09)  BINARY.
               10  PROFIT                     PIC S9(09)  BINARY.
               10  PROVISION                  PIC S9(09)  BINARY.
               10  APPORTIONMNT-EXCL-IND      PIC  9(01).
               10  HI-BAL-LF                  PIC  9(09)  BINARY.
               10  HI-DELQ-LF                 PIC  9(02)  BINARY.
               10  NUM-1-CYC-LF               PIC  9(03)  BINARY.
               10  NUM-2-CYC-LF               PIC  9(03)  BINARY.
               10  NUM-3-CYC-LF               PIC  9(03)  BINARY.
               10  NUM-4P-CYC-LF              PIC  9(03)  BINARY.
               10  NO-CARDS                   PIC  9(03)  BINARY.
               10  MONTHLY-SEGMENT OCCURS 12 TIMES
                  INDEXED BY RV-M-SEG-IDX.
                   15  DATE-CYCLE             PIC  9(08)  BINARY.
                   15  BLOCK-CODE             PIC  X(04).
                   15  CYCLE-BALANCE          PIC S9(09)  BINARY.
                   15  CASH-BALANCE           PIC S9(09)  BINARY.
                   15  AMOUNT-DUE             PIC S9(09)  BINARY.
                   15  VAL-PAYMENTS           PIC S9(09)  BINARY.
                   15  VAL-MERCH-SALES        PIC S9(09)  BINARY.
                   15  NUM-MERCH-SALES        PIC  9(03)  BINARY.
                   15  VAL-CASH-ADV           PIC S9(09)  BINARY.
                   15  NUM-CASH-ADV           PIC  9(03)  BINARY.
                   15  VAL-ARREARS            PIC S9(09)  BINARY.
                   15  LIMIT                  PIC S9(09)  BINARY.
                   15  VAL-RETURNS            PIC S9(09)  BINARY.
                   15  VAL-OTHER-TXNS         PIC S9(09)  BINARY.
                   15  VAL-INTEREST           PIC S9(09)  BINARY.
                   15  VAL-TOTAL-FEES         PIC S9(09)  BINARY.
                   15  VAL-OTHER-DEBITS       PIC S9(09)  BINARY.
                   15  NO-NSF                 PIC S9(03)  BINARY.
                   15  NUM-BALTRF             PIC S9(03)  BINARY.
                   15  VAL-BALTRF             PIC S9(09)  BINARY.
                   15  BAL-BALTRF             PIC S9(09)  BINARY.
                   15  VAL-INTCHG-REVNU       PIC S9(09)  BINARY.
               10  MONTHLY-SEGMENT2 OCCURS 24 TIMES
                      INDEXED BY RV-SEG2-IDX.
                   15  CYCLES-DELQ            PIC  9(01).
