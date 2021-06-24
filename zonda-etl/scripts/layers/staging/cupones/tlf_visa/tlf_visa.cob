       01  TLF.
           03 STAT                        PIC 9(02).
           03 PRODUCT_IND                 PIC X(02).
           03 RELEASE_NUMBER              PIC X(02).
           03 DPC_NUMBER                  PIC 9(04).
           03 1ER_USO                     PIC X(01).
           03 SEMSGTYP                    PIC 9(04).
           03 SEMCACCT                    PIC X(19).
           03 SEMTCODE                    PIC 99.
           03 SEMTFACC                    PIC 99.
           03 SEMTTACC                    PIC 99.
           03 SEMAMT1                     PIC S9(14)V99  COMP.
           03 SEMAMT2                     PIC S9(14)V99  COMP.
           03 SEMAMT3                     PIC S9(14)V99  COMP.
           03 FIID                        PIC X(04).
           03 AUTH_CODE                   PIC 9(06).
           03 SYS_ID                      PIC X(01).
           03 TRACE_NO                    PIC 9(06).
           03 SEMTRDAT                    PIC 9(04).
           03 SEMPDAT                     PIC 9(04).
           03 SEMTRTIM                    PIC 9(06).
           03 SEMCOD_TERM                 PIC X(04).
           03 SEMCOD_TARJ                 PIC X(04).
           03 SEMCCODE                    PIC X(03).
           03 SEMRREV                     PIC 99.
           03 ID_CANAL                    PIC X(03).
           03 MARCA_IVA                   PIC X(01).
           03 SEMRCARD                    PIC X.
           03 SEMTRNAD                    PIC XX.
           03 SEMCITY                     PIC X(15).
           03 T72_DNI_CUIL                PIC X(12).
           03 T72_COMISION                PIC S9(4)V99.
           03 SEG_TIPO_MOV                PIC X(1).
           03 SEG_TIPO_SEGURO             PIC X(1).
           03 SEG_IDENTIFICACION          PIC X(1).
           03 SEG_COSTO_COBERTURA         PIC 9(4)V9(2).
           03 SOR_BANELCO                 PIC X(1).
           03 SOR_BANELCO_IMPORTE         PIC 9(05)V99.
           03 SOR_BANCOS                  PIC X(1).
           03 SOR_BANCOS_IMPORTE          PIC 9(05)V99.
           03 SEMFALEN                    PIC X(02).
           03 SEMFANUM_DEB                PIC 9(12).
           03 SEMFANUM_CRED               PIC 9(16).
           03 SEMFANUM_SUC                PIC 9(03).
           03 SEMFANUM_CTA                PIC 9(07).
           03 SEMTALEN                    PIC X(02).
           03 SEMTANUM_DEB                PIC 9(12).
           03 SEMTANUM_CRED               PIC 9(16).
           03 SEMTANUM_SUC                PIC 9(03).
           03 SEMTANUM_CTA                PIC 9(07).
           03 SEMTERM_COUNTRY             PIC X(02).
           03 SEMBANC                     PIC X(04).
           03 SEMFIID                     PIC X(04).
           03 CBU_DESTINO                 PIC X(22).
           03 TITULARIDAD                 PIC X(01).
           03 TIPO_OPER                   PIC X(01).
           03 MAND_DOC_BENEF              PIC X(01).
           03 MAND_NUM_BENEF              PIC X(12).
           03 MAND_IDENTIF                PIC X(04).
           03 SEMTCOMER                   PIC X(25).
           03 SEMTATMI                    PIC X(16).
           03 SEMTRENU                    PIC 9(06).
           03 SEMBANC_1                   PIC X(04).
           03 PORCEN_RETENCION            PIC 9(05).
           03 PORCEN_PERCEPCION           PIC 9(05).
           03 RETENCION_IMP_PAIS          PIC S9(16)V99 COMP.
           03 TIPO_EXTR                   PIC X(01).
           03 SEMTIDEP                    PIC X.
           03 SEMTIDEB                    PIC X.
           03 SEMTIPAG                    PIC X.
           03 SEMTITAR                    PIC X.
           03 NOMBRE_ORIGEN               PIC X(18).
           03 TIP_DEBIN                   PIC X(02).
           03 ID_DEBIN                    PIC X(22).
           03 CUIT_BCO_CDOR               PIC X(12).
           03 CBU_BCO_CDOR                PIC X(22).
           03 CUIT_BCO_VDOR               PIC X(12).
           03 CBU_BCO_VDOR                PIC X(22).
           03 FEC_NEG_COEL                PIC X(08).
           03 SCORING_DEBIN               PIC 9(03).
           03 CPTO_DEBIN                  PIC 9(01).
           03 DESC_CPTO_DEB               PIC X(03).
           03 CORRESP_TITU                PIC X(01).
           03 MONEDA_ORIGEN               PIC X(3).
           03 SEMIMPO_ORIGINAL            PIC S9(16)V99 COMP.
           03 SEMFEFA                     PIC X(006).
           03 SEMCOTCO                    PIC 9(14)V99 COMP.
           03 SEMCOTVE                    PIC 9(14)V99 COMP.
           03 SEMAMT5                     PIC 9(14)V99 COMP.
           03 PERCEPCION_IMP_GANAN        PIC S9(16)V99 COMP.
           03 NRO_CONTROL                 PIC X(04).
           03 TRANS_BANCO_RECEPTOR        PIC X(04).
           03 TRANS_CODIGO_CONCEPTO       PIC X(01).
           03 TRANS_DESC_CONCEPTO         PIC X(03).
           03 TRANS_REFERENCIA            PIC X(12).
           03 SEMNRO_CARGO                PIC X(08).
           03 PORC_DEVL_CLTE              PIC X(03).
           03 PORC_DEVL_COMER             PIC X(03).
           03 RUBRO                       PIC X(04).
           03 BALANCEO                    PIC X(01).
           03 TIPO_CAJERO                 PIC X(01).
           03 FILLER                      PIC X(100).