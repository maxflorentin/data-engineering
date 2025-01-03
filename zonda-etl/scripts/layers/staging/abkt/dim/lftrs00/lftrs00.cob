          01  TS0.
               10  LOCATION                 	PIC X(6).
               10  GROUP-PREFIX         		PIC X(7).
               10  GROUP-NUMBER         		PIC 9(6).
               10  RECORD-TYPE              	PIC X(2).
               10  SEQUENCE                     PIC 9(3).
               10  FILLER-1                     PIC X(17).
               10  TYPE                         PIC X(01).
               10  SUFFIX                       PIC X(02).
               10  BENE-RECV-REFERENCE          PIC X(30).
               10  BENE-RECV-ACCT-NO            PIC X(15).
			   10  BENE-RECV-NAME-ADDR-LINE1    PIC X(35).
			   10  BENE-RECV-NAME-ADDR-LINE2    PIC X(35).
			   10  BENE-RECV-NAME-ADDR-LINE3    PIC X(35).
			   10  BENE-RECV-NAME-ADDR-LINE4    PIC X(35).
               10  BENE-RECV-CITY-CODE          PIC X(04).
               10  BENE-RECV-ACRONYM            PIC X(08).
               10  BENE-RECV-COUNTRY            PIC X(05).
               10  CUST-SEND-REFERENCE          PIC X(30).
               10  CUST-SEND-ACCT-NO            PIC X(15).
			   10  CUST-SEND-NAME-ADDR-LINE1    PIC X(35).
			   10  CUST-SEND-NAME-ADDR-LINE2    PIC X(35).
			   10  CUST-SEND-NAME-ADDR-LINE3    PIC X(35).
			   10  CUST-SEND-NAME-ADDR-LINE4    PIC X(35).
               10  CUST-SEND-CITY-CODE          PIC X(04).
               10  CUST-SEND-ACRONYM            PIC X(08).
               10  CUST-SEND-COUNTRY            PIC X(05).
               10  WRKR-NO                      PIC X(3).
               10  WRKR-PASSWORD                PIC X(8).
               10  TRN-FROM-INC-SWIFT           PIC X(1).
               10  TRN-FROM-CUST-BRIDGE         PIC X(1).
               10  LOG-MODE-IND                 PIC X(1).
               10  TEMPLATE-NUM                 PIC X(07).
               10  BENE-RECV-SW-ID              PIC X(12).
               10  CUST-SEND-SW-ID              PIC X(12).
               10  BENE-RECV-ID-IND         	PIC X(01).
               10  BENE-RECV-ID-TYPE        	PIC X(02).
               10  BENE-RECV-ID             	PIC X(34).
               10  CUST-SEND-ID-IND         	PIC X(01).
               10  CUST-SEND-ID-TYPE        	PIC X(02).
               10  CUST-SEND-ID             	PIC X(34).
               10  SEND-ID-IND              	PIC X(01).
               10  SEND-ID-TYPE             	PIC X(02).
               10  SEND-ID                  	PIC X(34).
               10  RECV-ID-IND              	PIC X(01).
               10  RECV-ID-TYPE             	PIC X(02).
               10  RECV-ID                  	PIC X(34).
               10  BENC-ORDR-ID-IND         	PIC X(01).
               10  BENC-ORDR-ID-TYPE        	PIC X(02).
               10  BENC-ORDR-ID             	PIC X(34).
               10  ORDR-INST-ID-IND         	PIC X(01).
               10  ORDR-INST-ID-TYPE        	PIC X(02).
               10  ORDR-INST-ID             	PIC X(34).
               10  INTRM-ID-IND             	PIC X(01).
               10  INTRM-ID-TYPE            	PIC X(02).
               10  INTRM-ID                 	PIC X(34).
               10  COVER-BENE-ID-IND        	PIC X(01).
               10  COVER-BENE-ID-TYPE       	PIC X(02).
               10  COVER-BENE-ID            	PIC X(34).
               10  AC-INST-ID-IND           	PIC X(01).
               10  AC-INST-ID-TYPE          	PIC X(02).
               10  AC-INST-ID               	PIC X(34).
               10  LIAB-SUB-NUMBER              PIC X(3).
               10  PROTOTYPE-NUMBER             PIC X(7).
               10  FILLER-2                     PIC X(970).