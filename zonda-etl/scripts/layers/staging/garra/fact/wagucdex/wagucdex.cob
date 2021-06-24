          ******************************************************************
          * WAGUXDEX                                                       *
          * FORMATO DEL ARCHIVO DE CONTRATOS DIARIO, SEMANAL Y MENSUAL     *
          ******************************************************************
           02 WAGUXDEX.
              10 NUM_PERSONA           PIC X(08).
              10 COD_ENTIDAD           PIC X(04).
              10 COD_CENTRO            PIC X(04).
              10 NUM_CONTRATO          PIC X(12).
              10 COD_PRODUCTO          PIC X(02).
              10 COD_SUBPRODU          PIC X(04).
              10 COD_DIVISA            PIC X(03).
              10 FEC_INISITIR          PIC X(10).
              10 FEC_DUDOSIDA          PIC X(10).
              10 COD_MARCA             PIC X(02).
              10 COD_SMARCGSI          PIC X(03).
              10 IMP_RIESMOLO   	   PIC 9(13)V9(4).
              10 IMP_IRREMOLO  	       PIC 9(13)V9(4).
              10 IMP_CASTMOLO   	   PIC 9(13)V9(4).
              10 IMP_PENDMOLO   	   PIC 9(13)V9(4).
              10 IMP_QUITMOLO   	   PIC 9(13)V9(4).
              10 NUM_POSICION          PIC X(11).
              10 FECHA_ALTAREG         PIC X(10).
              10 FEC_MARCSUBM          PIC X(10).
              10 FEC_ALT_PROD          PIC X(10).
              10 IMP_CAP_VENC          PIC 9(13)V9(4).
              10 IMP_INT_VENC          PIC 9(13)V9(4).
              10 IMP_OTR_TOT           PIC 9(13)V9(4).
              10 IMP_CAP_A_VENC        PIC 9(13)V9(4).
              10 FEC_VTO_PROD          PIC X(10).
              10 NRO_CUO_ACT           PIC 9(05).
              10 NRO_CUO_TOT           PIC 9(05).
              10 COD_AMORT_CAP         PIC X(02).
              10 COD_TIPOTASA          PIC X(02).
              10 COD_FINALIDA          PIC X(02).
              10 COD_DESTINO           PIC X(05).
              10 FEC_CAMBIO_NOR        PIC X(10).
              10 MOTIVO_RIESGO_SUB_EST PIC X(50).
              10 COD_ATRIB_SEG_ESP     PIC X(02).
              10 IND_ARRASTRE          PIC X(01).
              10 FEC_INIGRA            PIC X(10).
              10 FEC_FINGRA            PIC X(10).
              10 PLZ_PLAZO             PIC X(04).
              10 CON_DIAIMPAG          PIC 9(6).
              10 NRO_CUOTACO           PIC 9(4).
              10 CLA_GARANTIA          PIC X(03).
              10 IND_NORMALIZ          PIC X(01).
              10 IMP_INTER_ORD         PIC 9(13)V9(04).
              10 TIP_REESTRUCT         PIC X(01).
              10 COD_MARCA_SUBJETIVA   PIC X(01).
              10 IND_PROC_NOR          PIC X(01).
              10 IND_INCUMPLIM         PIC X(01).
              10 FEC_INCUMPLIM         PIC X(10).
