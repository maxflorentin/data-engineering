              ******************************************************************
              *  EMPRESA     :   BANCO RIO                                     *
              *  SISTEMA     :   CAMPA#AS                                      *
              *  COPY        :   INPUT_OUTPUT SCREENING EMPRESAS               *
              *  LONGITUD    :   320                                           *
              *  FECHA VERS.1:   ABRIL/2006                                    *
              *  PROGRAMADOR :   ALICIA CALVO / LEONARDO ZORNADA               *
              *  DESCRIPCION :   INTERFASE DE COMUNICACIäN BD MARKETING/RIESGOS*
              * ID2704_ SCREENING PARA BASE UNICA SE AGREGA FECHA_PROCESO    *
              * 21/01/2016 _ EMPRESAS                                        *
              ****************************************************************

           01 WACAM600.
                   10 TPO_INS                	PIC X(02).
                   10 CUIT_PREFIJO        		PIC X(02).
                   10 CUIT_NRO            		PIC X(08).
                   10 CUIT_DV             		PIC X(01).
                   10 TIPO_DOC               	PIC X(02).
                   10 NRO_DOC                	PIC X(11).
                   10 FEC_NACIMIENTO            PIC X(10).
                   10 MAR_SEXO                  PIC X(01).
                   10 NRO_EMPRESA               PIC X(05).
                   10 EST_CAMP                  PIC X(02).
                   10 PERS_CONDICION            PIC X(03).
                   10 NUP                       PIC X(08).
                   10 APELLIDO                  PIC X(30).
                   10 NOMBRE                    PIC X(30).
                   10 TIPO_PERS                 PIC X(01).
                   10 COD_PROD_05               PIC X(01).
                   10 COD_PROD_07               PIC X(01).
                   10 COD_PROD_14               PIC X(01).
                   10 COD_PROD_20               PIC X(01).
                   10 COD_PROD_21                PIC X(01).
                   10 COD_PROD_30               PIC X(01).
                   10 COD_PROD_31               PIC X(01).
                   10 COD_PROD_33               PIC X(01).
                   10 COD_PROD_34               PIC X(01).
                   10 COD_PROD_36               PIC X(01).
                   10 COD_PROD_39               PIC X(01).
                   10 COD_PROD_40               PIC X(01).
                   10 COD_PROD_70               PIC X(01).
                   10 COD_PROD_71               PIC X(01).
                   10 SUC_BANELCO               PIC X(04).
                   10 MAR_EMPLEADO              PIC X(01).
                   10 MAR_FIRMANTE              PIC X(01).
                   10 AFIR_SITU_RIO             PIC X(03).
                   10 AFIR_SITU_BCRA            PIC X(03).
                   10 FEVE_GYS_SITU             PIC X(02).
                   10 MORIA_SITU                PIC X(02).
                   10 MORIA_MON_CAST            PIC 9(13)V99.
                   10 MORIA_FEC_CASTIGO         PIC X(10).
                   10 AMAS_CTA_11               PIC X(01).
                   10 AMAS_MOTBAJ_05            PIC X(01).
                   10 CACS_ACUERDO              PIC X(01).
                   10 CACS_MON_MAX              PIC 9(13)V99.
                   10 CACS_DIA_MAX              PIC 9(03).
                   10 CACS_CAN_ATRASOS          PIC 9(03).
                   10 AMUC_CHEQ_NORESC          PIC X(01).
                   10 AMUC_CHEQ_RESC            PIC X(01).
                   10 FDM_SITU_MONIMP           PIC X(01).
                   10 AMAS_INHAB                PIC X(01).
                   10 AMAS_MOTBAJ_06            PIC X(01).
                   10 UGE_PRESTAMO_N_BULLET     PIC X(01).
                   10 UGE_PRESTAMO_BULLET       PIC X(01).
                   10 ACOB_PORC_CANT            PIC 9(03)V99.
                   10 ACOB_PORC_MON             PIC 9(03)V99.
                   10 SGE_PROPUESTA             PIC X(02).
                   10 SGE_SEGM                  PIC X(04).
                   10 SGE_CANT_LINEAS           PIC 9(02).
                   10 SGE_PROD_01               PIC X(02).
                   10 SGE_PROD_02               PIC X(02).
                   10 SGE_PROD_03               PIC X(02).
                   10 SGE_PROD_04               PIC X(02).
                   10 SGE_PROD_05               PIC X(02).
                   10 SGE_PROD_06               PIC X(02).
                   10 SGE_PROD_07               PIC X(02).
                   10 SGE_PROD_08               PIC X(02).
                   10 SGE_PROD_09               PIC X(02).
                   10 SGE_PROD_10               PIC X(02).
                   10 ESPACIO_USER              PIC X(22).
                   10 FECHA_PROCESO             PIC X(10).
                   10 FILLER                    PIC X(48).
