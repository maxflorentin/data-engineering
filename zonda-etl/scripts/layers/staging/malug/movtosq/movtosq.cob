	      01 DMOV.                                                      
            10 ENTIDAD            PIC X(04).                           
            10 OFICINA            PIC X(04).                           
            10 CUENTA             PIC X(12).                                                      
            10 NIO                PIC X(24).                           
            10 CODCONLI           PIC X(03).                           
            10 FEOPER             PIC X(10).                           
            10 PRODUCTO           PIC X(02).                           
            10 SUBPRO             PIC X(04).                           
            10 TIP_PRODUCTO       PIC X(01).
            10 COD_EVENTO         PIC X(04).
            10 TIPOCPTO           PIC X(01).                           
            10 DES_CODCONLI       PIC X(20).
            10 TIPO_MOV           PIC X(01).
            10 NUMREC             PIC S9(05)        COMP-3.            
            10 NUM_SECUENCIA      PIC S9(3)V  USAGE COMP-3.
            10 IMP_BASE           PIC S9(13)V9(04)  COMP-3.
            10 IND_LIQIMPUE       PIC X(01).
            10 POR_ALICUOTA       PIC S9(03)V9(06)  COMP-3.
            10 FECALMORA          PIC X(10).                           
            10 SITDEUCT           PIC X(02).                           
            10 TIP_CONDONAR       PIC X(05).
            10 FECONTA            PIC X(10).                           
            10 FELIQ              PIC X(10).                           
            10 FEVALOR            PIC X(10).                                            
            10 CLAOPER            PIC X(02).                           
            10 ENTIOPE            PIC X(04).                           
            10 OFIOPE             PIC X(04).                                              
            10 IMPMOVI            PIC S9(13)V9(04) COMP-3.             
            10 COD_DIVISA         PIC X(03).
            10 TIP_CAMBIO_OPE     PIC S9(06)V9(05) COMP-3.
            10 SALREAL            PIC S9(13)V9(4) USAGE COMP-3.        
            10 NUN_INCIDEN        PIC S9(9)V USAGE COMP-3.
            10 IND_FORMPAGO       PIC X(01).
            10 IMP_PAGO           PIC S9(13)V9(04) COMP-3.
            10 COD_DIVI_PAGO      PIC X(03).
            10 INDRETRO           PIC X(01).                           
            10 FECRETRO           PIC X(10).
            10 COD_ENTCHEQU   PIC X(04).
            10 COD_OFICHEQU   PIC X(04).
            10 COD_CTACHEQU   PIC X(12).
            10 NUM_DOCCHEQU   PIC S9(13) COMP-3.
            10 TIP_DOCCHEQU   PIC X(02).
            10 FEC-DISPCHEQU  PIC X(10).
            10 COD_PLAZA      PIC X(08).
            10 ENTIDAD_PAG        PIC X(04).
            10 CENTRO_PAG         PIC X(04).
            10 CUENTA_PAG         PIC X(12).
            10 DIGICCC1_PAG       PIC X(01).
            10 DIGICCC2_PAG       PIC X(01).
            10 TBL_ORIGEN         PIC X(03).
            10 REND_SEG_VIDA      PIC S9(13)V9(04) COMP-3.
            10 FILLER                  PIC X(46).                                                  
            10 ENTIDAD_UMO        PIC X(04).
            10 CENTRO_UMO         PIC X(04).
            10 USERID_UMO         PIC X(08).
            10 NETNAME_UMO        PIC X(08).
            10 TIMESTAMP          PIC X(26).          