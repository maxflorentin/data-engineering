      ******************************************************************        
      * INTERFAZ A RIO 46 _ DETALLE DE LOS CONCEPTOS DE UNA CANCELACION*        
      * JUNTA LA MDDTCAR, MDDTCCR Y MDDTCIR                            *        
      ******************************************************************        
       02  MDEC160R.                                                            
           10 COD_ENTIGEN               PIC X(4).                      
           10 IDF_CANCELAC              PIC X(24).                     
           10 COD_ENTIDAD               PIC X(4).                      
           10 COD_CENTRO                PIC X(4).                      
           10 NUM_CONTRATO              PIC X(12).                     
           10 COD_PRODUCTO              PIC X(2).                      
           10 COD_SUBPRODU              PIC X(4).                      
           10 COD_DIVISA                PIC X(3).                      
           10 NUM_RECIBO                PIC 9(5).                      
           10 IMP_CANCRECI              PIC 9(13)V9(4).                
           10 IMP_CONDRECI              PIC 9(13)V9(4).                
           10 COD_ESTRECIA              PIC X(1).                      
           10 COD_ESTRECI               PIC X(1).                      
           10 IND_FAC_ONLN              PIC X(1).                      
           10 FEC_ULTPGINA              PIC X(10).                     
           10 FEC_ULTPGINT              PIC X(10).                     
           10 COD_COEFINDE              PIC X(4).                      
           10 FAC_INDEXA                PIC 9(3)V9(6).                 
           10 FEC_ULTINDXA              PIC X(10).                     
           10 FEC_ULTINDEX              PIC X(10).                     
           10 INT_TCPSTEOR              PIC 9(3)V9(5).                 
           10 POR_MORTEOR               PIC 9(3).                      
           10 INT_TCPSAPLI              PIC 9(3)V9(5).                 
           10 POR_MORAPLI               PIC 9(3).                      
           10 POR_AJUSAPLI              PIC 9(3)V9(5).                 
           10 TOTAL_CONTABLE            PIC 9(13)V9(4).                
           10 TOTAL_NO_CONTABLE         PIC 9(13)V9(4).                
           10 IMPDEV_CAPITAL            PIC 9(13)V9(4).                
           10 IMPDEV_INTERES            PIC 9(13)V9(4).                
           10 IMPDEV_AJUSTE             PIC 9(13)V9(4).                
           10 IMPDEV_AJUSTE_1           PIC 9(12)V9(4).
           10 IMPDEV_AJUSTE_SIGNO       PIC X(01).                     
           10 IMPUDEV_TOTAL             PIC 9(13)V9(4).                
           10 IMPUDEV_IVA1              PIC 9(13)V9(4).                
           10 IMPUDEV_IVA2              PIC 9(13)V9(4).                
           10 IMPUDEV_ING_B             PIC 9(13)V9(4).                
           10 IMPUDEV_IMP_E             PIC 9(13)V9(4).                
           10 IMPUDEV_OTRO              PIC 9(13)V9(4).                
           10 IMPERC_COMISION           PIC 9(13)V9(4).                
           10 IMPERC_SEG_VIDA           PIC 9(13)V9(4).                
           10 IMPERC_SEG_INCE           PIC 9(13)V9(4).                
           10 IMPERC_SEG_TOTAL          PIC 9(13)V9(4).                
           10 IMPERC_GASTOS_MD_UG       PIC 9(13)V9(4).                
           10 IMPERC_INT_CPS            PIC 9(13)V9(4).                
           10 IMPERC_INT_MOR            PIC 9(13)V9(4).                
           10 IMPERC_AJUSTE             PIC 9(13)V9(4).                
           10 IMPERC_INTERES            PIC 9(13)V9(4).                
           10 IMPUPERC_TOTAL            PIC 9(13)V9(4).                
           10 IMPUPERC_IVA1             PIC 9(13)V9(4).                
           10 IMPUPERC_IVA2             PIC 9(13)V9(4).                
           10 IMPUPERC_ING_B            PIC 9(13)V9(4).                
           10 IMPUPERC_IMP_E            PIC 9(13)V9(4).                
           10 IMPUPERC_OTRO             PIC 9(13)V9(4).                