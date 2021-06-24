*****************************************************************
* NOMBRE ARCHIVO.......: PROMOCIONES_AR                         *
* LONGITUD DE REGISTRO.: 500 CARACTERES                         *
* ORGANIZACION.........:                                        *
*                                                               *
* CLAVES                                                        *
* ------> PRINCIPAL....:                                        *
* ------> ALTERNATIVA 1:                                        *
*****************************************************************      
01 OPS.                                     
   02 CODBCO                     PIC 9(03).     
   02 COD-VISA                   PIC X(03).     
   02 COD-BANCO                  PIC X(04).     
   02 DESC-CAMPANA               PIC X(30).     
   02 NUMEST                     PIC 9(10).     
   02 DESEST                     PIC X(30).     
   02 RUBRO-COD                  PIC 9(04).     
   02 RUBRO-DESC                 PIC X(30).     
   03 ESTAB-CALLE                PIC X(30).     
   03 ESTAB-NRO                  PIC 9(05).    
   03 ESTAB-PISO                 PIC X(02).     
   03 ESTAB-TIPO                 PIC X(03).     
   03 ESTAB-DEPTO                PIC X(04).     
   03 ESTAB-CP                   PIC X(04).     
   03 ESTAB-LOCA                 PIC X(30).     
   03 ESTAB-PROV                 PIC X(30).     
   02 ESTADO-EST                 PIC X(01).     
   02 COMERCIO-NRO               PIC 9(10).     
   02 COMERCIO-DESC              PIC X(30).     
   03 PROD-CRED                  PIC X(01).    
   03 PROD-DEB                   PIC X(01).    
   03 PROD-AMEX                  PIC X(01).    
   03 PROD-REGALO                PIC X(01).    
   03 DOM-PROM                   PIC X(01).    
   03 LUN-PROM                   PIC X(01).    
   03 MAR-PROM                   PIC X(01).    
   03 MIE-PROM                   PIC X(01).    
   03 JUE-PROM                   PIC X(01).    
   03 VIE-PROM                   PIC X(01).    
   03 SAB-PROM                   PIC X(01).    
   02 FORM-PAG                   PIC X(01).    
   03 ICF-ESTADO                 PIC 9(01).     
   03 ICF-DESDE                  PIC 9(02).
   03 ICF-HASTA                  PIC 9(02).     
   03 DESC-USU                   PIC 9(02)V9.   
   03 DESC-EST                   PIC 9(02)V9.   
   03 DESC-DESDE                 PIC 9(02).     
   03 DESC-HASTA                 PIC 9(02).     
   02 DESDE-ESTAB                PIC X(08).     
   02 HASTA-ESTAB                PIC X(08).     
   02 DESDE-CAMPANA              PIC X(08).     
   02 HASTA-CAMPANA              PIC X(08).     
   02 ALTA-ESTAB                 PIC X(08).     
   02 BAJA-ESTAB                 PIC X(08).    
   03 SEGM-USU                   PIC X(01).   
   03 SEGM-USU-TIPO              PIC 9(01).   
   03 SEGM-USU-VALOR01           PIC X(04).   
   03 SEGM-USU-VALOR02           PIC X(04).  
   03 SEGM-USU-VALOR03           PIC X(04).   
   03 SEGM-USU-VALOR04           PIC X(04).    
   03 SEGM-USU-VALOR05           PIC X(04).    
   03 SEGM-USU-VALOR06           PIC X(04).   
   03 SEGM-USU-VALOR07           PIC X(04).   
   03 SEGM-USU-VALOR08           PIC X(04).   
   03 SEGM-USU-VALOR09           PIC X(04).     
   03 SEGM-USU-VALOR10           PIC X(04).     
   03 TOPE-TIPO                  PIC X(01).     
   03 TOPE-MONTO                 PIC 9(05)V99.  
   03 VALOR-FIJO-TIPO            PIC X(01).     
   03 VALOR-FIJO-MONTO           PIC 9(05)V99.  
   03 VALOR-FIJO-MINIMO          PIC 9(05)V99.  
   02 SEGM-DEBITO                PIC 9(01).     
   02 FUNC-ALTA-ORIGEN           PIC X(08).     
   02 TIPO-ALTA-ORIGEN           PIC X(01).     
   02 FILLER               PIC X(87).     
******************************************************************        
* THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 62      *        
******************************************************************        
     