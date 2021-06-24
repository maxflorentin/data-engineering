           02  DCO.
               10  LENGTH             PIC S9(08) BINARY.
               10  TENANT-ID          PIC S9(08) BINARY.
               10  CUSTOMER-ID        PIC  X(20).
               10  REC-ID             PIC  X(02).
               10  REC-ID-NUM         PIC  9(02).
               10  REC-SEQ-NUM        PIC  9(02).
               10  ACCOUNT-ID         PIC  X(20).
               10  PROD-TYPE      PIC  9(01).
               10  PROD-CODE              PIC  9(03)  BINARY.
               10  STATUS                 PIC  9(03)  BINARY.
               10  REASON-CLOSED          PIC  9(01).
               10  LIABILITY-IND          PIC  9(01).
               10  CF-EXCLUSION-IND       PIC  9(01).
               10  CF-INC-EXCLUSION-IND   PIC  9(01).
               10  CF-DEC-EXCLUSION-IND   PIC  9(01).
               10  CDA-EXCLUSION-IND      PIC  9(01).
               10  AU-EXCLUSION-IND       PIC  9(01).
               10  PYMT-RESCHED-TODAY-IND PIC  9(01).
               10  PAYMENT-METHOD         PIC  9(01).
               10  FULL-BAL-PAYMENT-IND   PIC  9(01).
               10  RATE-TIER              PIC  9(02).
               10  TRANS-REVOLVE-IND      PIC  9(01).
               10  TELEPHONE-IND          PIC  9(01).
               10  ADDRESS-IND            PIC  9(01).
               10  SMS-IND                PIC  9(01).
               10  BLOCK-CODE             PIC  X(04).
               10  LEGAL-CODE             PIC  9(03)  BINARY.
               10  ORIGINATION-SOURCE     PIC  9(01).
               10  PYMT-SOURCE            PIC S9(08)  BINARY.
               10  PYMT-TYPE              PIC S9(08)  BINARY.
               10  DATE-OPEN              PIC  9(08)  BINARY.
               10  DATE-CLOSED            PIC  9(08)  BINARY.
               10  DATE-BILLING-CYMD      PIC  9(08)  BINARY.
               10  DATE-START-OVERLIMIT   PIC  9(08)  BINARY.
               10  DATE-START-DELQ        PIC  9(08)  BINARY.
               10  DATE-START-DEBIT-BAL   PIC  9(08)  BINARY.
               10  DATE-DEFAULT-NOTICE    PIC  9(08)  BINARY.
               10  DATE-NSF               PIC  9(08)  BINARY.
               10  DATE-LAST-REAGE        PIC  9(08)  BINARY.
               10  DATE-BNKRPT-CHGOFF     PIC  9(08)  BINARY.
               10  DATE-ANNIV             PIC  9(08)  BINARY.
               10  DATE-LAST-DEBIT        PIC  9(08)  BINARY.
               10  DATE-LAST-CREDIT       PIC  9(08)  BINARY.
               10  DATE-LAST-CRLN-OFFER   PIC  9(08)  BINARY.
               10  DATE-LAST-CRLN-INCREASE PIC  9(08)  BINARY.
               10  DATE-LAST-CRLN-DECREASE
                                          PIC  9(08)  BINARY.
               10  DATE-LAST-MON-TXN-CYM  PIC  9(06)  BINARY.
               10  DATE-LAST-CASH-CYM     PIC  9(06)  BINARY.
               10  DATE-LAST-DELQ-CYMD    PIC  9(08)  BINARY.
               10  DATE-LAST-REVIEW-CYM   PIC  9(06)  BINARY.
               10  DATE-LAST-PUR-CYM      PIC  9(06)  BINARY.
               10  DATE-LAST-TERMS-CHG-CYM
                                          PIC  9(06)  BINARY.
               10  DATE-FEE-CYM           PIC  9(06)  BINARY.
               10  DATE-EXPR-CYM          PIC  9(06)  BINARY.
               10  DATE-TERMS-EXPR-CYM    PIC  9(06)  BINARY.
               10  DATE-ORIGINAL-MATURITY PIC  9(06)  BINARY.
               10  DATE-CURRENT-MATURITY  PIC  9(06)  BINARY.
               10  DATE-SECURITY-CYM      PIC  9(06)  BINARY.
               10  DATE-DISC-START-CYMD   PIC  9(08)  BINARY.
               10  DATE-DISC-END-CYMD     PIC  9(08)  BINARY.
               10  DATE-PROM-BRKN-CYMD    PIC  9(08)  BINARY.
               10  DATE-DEFAULT-START     PIC  9(08)  BINARY.
               10  BHVR-SCORE             PIC S9(07)  BINARY.
               10  BHVR-RAW-SCORE         PIC S9(07)  BINARY.
               10  BHVR-SCRD-ID           PIC S9(04)  BINARY.
               10  BAR-FACTOR             PIC S9(01)V9(04)
                                                      BINARY.
               10  BALANCE                PIC S9(09)  BINARY.
               10  LIMIT                  PIC S9(09)  BINARY.
               10  CASH-BALANCE           PIC S9(09)  BINARY.
               10  CASH-LIMIT             PIC S9(09)  BINARY.
               10  SHADOW-LIMIT           PIC S9(09)  BINARY.
               10  CUST-AVAILABLE-LIMIT   PIC S9(09)  BINARY.
               10  AMT-ARREARS            PIC S9(09)  BINARY.
               10  AMT-DISPUTE            PIC S9(09)  BINARY.
               10  AMT-LAST-CREDIT        PIC S9(09)  BINARY.
               10  HIGH-BALANCE-LF        PIC S9(09)  BINARY.
               10  NUM-PYMNTS-LF          PIC  9(03)  BINARY.
               10  AMT-PAY-7-DAYS         PIC S9(09)  BINARY.
               10  AMT-PAY-15-DAYS        PIC S9(09)  BINARY.
               10  AMT-PAY-25-DAYS        PIC S9(09)  BINARY.
               10  NUM-PMTS-CTD           PIC S9(04)  BINARY.
               10  NUM-NSF-CTD            PIC S9(04)  BINARY.
               10  AMT-NSF-CTD            PIC S9(09)  BINARY.
               10  AMT-LAST-NSF           PIC S9(09)  BINARY.
               10  AMT-CURR-PYMT          PIC S9(09)  BINARY.
               10  AVG-PMT2-CTD           PIC S9(09)  BINARY.
               10  AVG-PMT3-CTD           PIC S9(09)  BINARY.
               10  AVG-PMT6-CTD           PIC S9(09)  BINARY.
               10  AVG-PMT12-CTD          PIC S9(09)  BINARY.
               10  NUM-CASH-ADV1-CTD      PIC S9(04)  BINARY.
               10  AMT-CASH-ADV1-CTD      PIC S9(09)  BINARY.
               10  DEFAULT-AMOUNT         PIC S9(09)  BINARY.
               10  DEFAULT-RECOVERY-AMT   PIC S9(09)  BINARY.
               10  DEFAULT-TOT-RCVRY-AMT  PIC S9(09)  BINARY.

               10  NUM-PTP                PIC S9(04)  BINARY.

               10  FINANCIAL-DATA-6       OCCURS 6 TIMES
                                     INDEXED BY CO-FIN-6-IDX.
                   20  MTHLY-BALANCE      PIC S9(09)  BINARY.
                   20  MTHLY-DEBITS       PIC S9(09)  BINARY.
                   20  MTHLY-CREDITS      PIC S9(09)  BINARY.
                   20  MTHLY-NUM-NSF      PIC  9(02).
               10  FINANCIAL-DATA-12      OCCURS 12 TIMES
                                     INDEXED BY CO-FIN-12-IDX.
                   20  DELQ               PIC  9(02).
                   20  MTHLY-INTEREST     PIC S9(09)  BINARY.
                   20  MTHLY-PROFIT       PIC S9(09)  BINARY.
                   20  MTHLY-REVENUE      PIC S9(09)  BINARY.
                   20  MTHLY-NUM-DEBITS   PIC  9(02).
                   20  MTHLY-FEES         PIC S9(09)  BINARY.
                   20  MTHLY-REBATES      PIC S9(09)  BINARY.
                   20  BAL-XFER-BAL       PIC S9(09)  BINARY.
                   20  BAL-XFER-NUM       PIC S9(04)  BINARY.
               10  REMAINING-TERM         PIC S9(03)  BINARY.
               10  ORIGINAL-LOAN-AMT      PIC S9(09)  BINARY.
               10  VAL-SECURITY           PIC S9(09)  BINARY.
               10  RESIDUAL-VALUE         PIC S9(09)  BINARY.
               10  PRIOR-REPOSSESSION-IND PIC  9(01).
               10  INSURANCE-IND          PIC  9(01).
               10  SS-DAYS-IN-COLL-PERIOD PIC  9(02).
               10  COLLECTIONS-STAGE      PIC  9(01).
               10  MANUAL-HANDLING-STATUS PIC  9(03)  BINARY.
               10  STATUS-AND-LTR-ACTION  PIC  9(01).
               10  CONTACT-MADE-IND       PIC  9(01).
               10  USR-DF-COLL-CODE-IND   PIC  9(04)  BINARY.
               10  USR-DF-COLL-AMT        PIC S9(09)  BINARY.
               10  USR-DF-AMT-TOO-TRIVIAL PIC S9(09)  BINARY.
               10  USR-DF-AMT-ENOUGH      PIC S9(09)  BINARY.
               10  USR-DF-DELQ-PCT-TRIVIAL PIC S9(09) BINARY.
               10  USR-DF-OVLM-PCT-TRIVIAL PIC S9(09) BINARY.
               10  USR-DF-WORSE-TRIGGER1  PIC S9(09)  BINARY.
               10  USR-DF-WORSE-TRIGGER2  PIC  9(01).
               10  USR-DF-WORSE-TRIGGER3  PIC S9(09)  BINARY.
               10  USR-DF-WORSE-TRIGGER4  PIC  9(01).
               10  USR-DF-BETTER-TRIGGER1 PIC S9(09)  BINARY.
               10  USR-DF-BETTER-TRIGGER2 PIC  9(01).
               10  USR-DF-BETTER-TRIGGER3 PIC S9(09)  BINARY.
               10  USR-DF-BETTER-TRIGGER4 PIC  9(01).
               10  USR-DF-EVENT           PIC  9(01).
               10  STGY-ID                PIC S9(03)  BINARY.
               10  SCEN-ID                PIC S9(04)  BINARY.
               10  ACTION-CTR             PIC  9(01).
               10  PTP                    PIC  9(01).
               10  NSF-TODAY-IND          PIC  9(01).
               10  CO-EXCLUSION-IND       PIC  9(01).
               10  DATE-BILL-EQV          PIC  9(08)  BINARY.
               10  DATE-FIRST-COLLS-DA    PIC  9(08)  BINARY.
               10  COLL-BALANCE-INITIAL   PIC S9(09)  BINARY.
               10  COLL-BALANCE-PREV      PIC S9(09)  BINARY.
               10  OOO-TYPE-PREV          PIC  9(01).
               10  DELQ-PREV              PIC  9(02).
               10  AMT-ARREARS-PREV       PIC S9(09)  BINARY.
               10  AMT-EXCESS-OVLM-PREV   PIC S9(09)  BINARY.
               10  BALANCE-PREV           PIC S9(09)  BINARY.
               10  BALANCE-LAST-REVIEW    PIC S9(09)  BINARY.
               10  LIMIT-PREV             PIC S9(09)  BINARY.
               10  LIMIT-LAST-REVIEW      PIC S9(09)  BINARY.
               10  PTP-PREV               PIC  9(01).
               10  TELEPHONE-IND-PREV     PIC  9(01).
               10  ADDRESS-IND-PREV       PIC  9(01).
               10  BLOCK-CODE-PREV        PIC  X(04).
               10  BLOCK-CODE-LAST-REVIEW PIC  X(04).
               10  WORST-CYC-DELQ-PREV    PIC  9(02).
               10  TOTAL-OOO-AMT-PREV     PIC S9(09)  BINARY.
               10  NUM-OOO-PREV           PIC  9(03)  BINARY.
               10  PREV-NONMON-SPID       PIC S9(04)  BINARY.
               10  PREV-NONMON-RDG-LOWLMT
                                      PIC S9(04)  BINARY.
               10  PREV-NONMON-RDG-UPLMT
                                      PIC S9(04)  BINARY.
               10  PREV-NONMON-STRATEGY   PIC S9(03)  BINARY.
               10  PREV-NONMON-SCENARIO   PIC S9(04)  BINARY.
               10  PREV-NONMON-SCEN-SEG   PIC X(05).
               10  FILLER                 PIC X(05).
