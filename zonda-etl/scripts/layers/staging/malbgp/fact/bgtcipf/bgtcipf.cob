
                02  IPF.
                    10  IPF_ENTIDAD         	 PIC X(4).
                    10  IPF_CENTRO_ALTA     	 PIC X(4).
                    10  IPF_CUENTA          	 PIC X(12).
                    10  IPF_SECUENCIA            PIC S9(5)V COMP-3.
                    10  IPF_SECUENCIA_REN        PIC S9(5)V COMP-3.
                    10  IPF_PRODUCTO             PIC X(2).
                    10  IPF_SUBPRODU             PIC X(4).
                    10  IPF_ULT_AVI_ASIG         PIC S9(3)V COMP-3.
                    10  IPF_TARIFA_VIG           PIC X(4).
                    10  IPF_PLAZO                PIC S9(5)V COMP-3.
                    10  IPF_PERIOD_LIQ           PIC X(1).
                    10  IPF_PERIOD_REA           PIC X(1).
                    10  IPF_DIVISA               PIC X(3).
                    10  IPF_PLAZO_ORI            PIC S9(5)V COMP-3.
                    10  IPF_IND_CTA_ASO          PIC X(1).
                    10  IPF_IND_EMI_DTO          PIC X(1).
                    10  IPF_DIAS_LIQ             PIC X(1).
                    10  IPF_IND_ESTADO           PIC X(1).
                    10  IPF_IND_GARAN            PIC X(1).
                    10  IPF_IND_INMOV            PIC X(1).
                    10  IPF_IND_NOPAGO           PIC X(1).
                    10  IPF_IND_BLOQUEO          PIC X(1).
                    10  IPF_SALDO_INICIAL        PIC S9(13)V99 COMP-3.
                    10  IPF_SALDO_INIC_UR        PIC S9(13)V9(4) COMP-3.
                    10  IPF_CODIGO_UR            PIC X(3).
                    10  IPF_SALDO                PIC S9(13)V99 COMP-3.
                    10  IPF_SALDO_UR             PIC S9(13)V9(4) COMP-3.
                    10  IPF_SALDO_RETEN          PIC S9(13)V99 COMP-3.
                    10  IPF_IND_TIPO_LIQ         PIC X(1).
                    10  IPF_TIPOINT              PIC S9(3)V9(5) COMP-3.
                    10  IPF_INC_PORC_VIG         PIC S9(3)V9(5) COMP-3.
                    10  IPF_IND_CANVENC          PIC X(1).
                    10  IPF_IND_CAPITA_IN        PIC X(1).
                    10  IPF_IND_CAPITA_REA       PIC X(1).
                    10  IPF_EJECUTIVO_COM        PIC X(4).
                    10  IPF_PLAN_COMISION        PIC X(4).
                    10  IPF_CANAL_APERTURA       PIC X(2).
                    10  IPF_IND_TRANSFER         PIC X(1).
                    10  IPF_IND_FRACCONC         PIC X(1).
                    10  IPF_IND_CUSTODIA         PIC X(1).
                    10  IPF_SUC_ING_CUSTOD       PIC X(4).
                    10  IPF_SUC_EGR_CUSTOD       PIC X(4).
                    10  IPF_IND_ORIGEN           PIC X(1).
                    10  IPF_OBSERVACIONES        PIC X(30).
                    10  IPF_IND_DOCUMENTO        PIC X(1).
                    10  IPF_IND_ACUERDO          PIC X(1).
                    10  IPF_RENTA_PROGR          PIC S9(13)V99 COMP-3.
                    10  IPF_ENTIDAD_CONT         PIC X(4).
                    10  IPF_CENTRO_GESTOR        PIC X(4).
                    10  IPF_NUMER_MOV            PIC S9(9)V COMP-3.
                    10  IPF_NUM_DOCUM            PIC S9(13)V COMP-3.
                    10  IPF_TIPO_DOC             PIC X(2).
                    10  IPF_PAGO                 PIC X(1).
                    10  IPF_IMPORTE_CAJA         PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_TELEP        PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_CTA          PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_CHEQUE       PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_ACREED       PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_CANDEP       PIC S9(13)V99 COMP-3.
                    10  IPF_IMPORTE_SIN_CTA      PIC S9(13)V99 COMP-3.
                    10  IPF_CANCEL               PIC X(1).
                    10  IPF_MOTIVO_CANCEL        PIC X(2).
                    10  IPF_FECHA_ALTA           PIC X(10).
                    10  IPF_FECHA_OPERA          PIC X(10).
                    10  IPF_FECHA_PAGO           PIC X(10).
                    10  IPF_FECHA_ULTREN         PIC X(10).
                    10  IPF_FECHA_ANULA          PIC X(10).
                    10  IPF_FECHA_PROVEN         PIC X(10).
                    10  IPF_FECHA_CANCEL         PIC X(10).
                    10  IPF_FECHA_PROLIQ         PIC X(10).
                    10  IPF_FECHA_ULTLIQ         PIC X(10).
                    10  IPF_FECHA_PROPER         PIC X(10).
                    10  IPF_FECHA_ULTPER         PIC X(10).
                    10  IPF_FECHA_ULTREA         PIC X(10).
                    10  IPF_FECHA_PROREA         PIC X(10).
                    10  IPF_FECHA_INGCUS         PIC X(10).
                    10  IPF_FECHA_EGCUS          PIC X(10).
                    10  IPF_FECHA_ULTIMP         PIC X(10).
                    10  IPF_TARIFA_RENOV         PIC X(4).
                    10  IPF_INC_PORC_RENOV       PIC S9(3)V9(5)  COMP-3.
                    10  IPF_INTEABO              PIC S9(13)V99   COMP-3.
                    10  IPF_IMPUESTO             PIC S9(13)V99   COMP-3.
                    10  IPF_INTPENDABO           PIC S9(13)V9999 COMP-3.
                    10  IPF_REAABO               PIC S9(13)V99   COMP-3.
                    10  IPF_INT-PERIOD           PIC S9(13)V99   COMP-3.
                    10  IPF_REA-PERIOD           PIC S9(13)V99   COMP-3.
                    10  IPF_RESP-IMPU            PIC X(1).
                    10  IPF_ENTIDAD-UMO          PIC X(4).
                    10  IPF_CENTRO-UMO           PIC X(4).
                    10  IPF_USERID-UMO           PIC X(8).
                    10  IPF_NETNAME-UMO          PIC X(8).
                    10  IPF_TIMEST-UMO           PIC X(26).