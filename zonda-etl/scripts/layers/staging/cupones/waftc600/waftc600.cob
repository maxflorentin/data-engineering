      ******************************************************************
      * NOMBRE ARCHIVO......: WAFTC600                                 *
      * DESCRIPCION.........: VISA - NOVEDADES PRESENT. Y FACTURACION  *
      * ORGANIZACION........: SAM                                      *
      * LONGITUD DE REGISTRO: 420 CARACTERES                           *
      ******************************************************************
      *
       02 600.
          03 BCODEST              PIC 9(03).
          03 CASADEST             PIC 9(03).
          03 CARTERA              PIC 9(02).
          03 USUARIO              PIC 9(10).
          03 FEVENTO              PIC 9(08).
          03 FPRES                PIC 9(08).
          03 FPAGO                PIC 9(08).
          03 MONEDA               PIC X(03).
          03 CODOP                PIC 9(06).
          03 EVENTO               PIC 9(03).
          03 EVENTO-GRUPO         PIC 9(02).
          03 EVENTO-ORIGEN        PIC 9(02).
          03 ORDEN                PIC 9(04).
          03 SIGNO-EVENTO         PIC X(01).
          03 FILLER                        PIC X(08).
          03 IMPORTE-1            PIC S9(08)V99.
          03 IMPORTE-2            PIC S9(08)V99.
          03 IMPORTE-3            PIC S9(08)V99.
          03 IMPORTE-4            PIC S9(08)V99.
          03 NUP                  PIC X(08).
          03 TIPDOC               PIC X(02).
          03 NUMDOC               PIC X(11).
          03 FILLER                        PIC X(07).
          03 MARCE-RLIMPIO        PIC X(01).
          03 BCODEST-E-I          PIC 9(03).
          03 CASADEST-E-I         PIC 9(03).
          03 CARTERA-E-I          PIC 9(02).
          03 FCIERRE              PIC 9(08).
          03 PLANCUOT             PIC 9(02).
          03 NRO-CUOTA            PIC 9(02).
          03 PRODUCTO-CONC        PIC X(02).
          03 PRODUCTO             PIC X(06).
          03 CODOP-ORIG           PIC 9(06).
          03 FORIG                PIC 9(08).
          03 FPRES-ORIG           PIC 9(08).
          03 FPAGO-ORIG           PIC 9(08).
          03 NUMCOMP              PIC X(08).
          03 CODAUT               PIC X(08).
          03 DB-AUTOMAT           PIC X(01).
          03 TERM-CAPTURA         PIC X(01).
          03 ATM                  PIC X(01).
          03 NUMEST               PIC 9(10).
          03 NUMCOM               PIC 9(10).
          03 BCO-PAGADOR          PIC X(01).
          03 BCOEST               PIC 9(03).
          03 SASAEST              PIC 9(03).
          03 RUBRO                PIC 9(04).
          03 CODGEO-EST           PIC 9(05).
          03 NOMCDAD              PIC X(13).
          03 DENEST               PIC X(32).
          03 FILLER                      PIC X(18).
          03 CODPAIS              PIC X(02).
          03 MONORIG              PIC X(03).
          03 IMPORIG              PIC S9(11)V99.
          03 CINTA                PIC X(06).
          03 TARJETA              PIC X(19).
          03 NRO-RECL-BCO         PIC X(10).
          03 TNA-EMISOR           PIC 9(03)V9(02).
          03 CUIT-ESTAB           PIC X(13).
          03 IMP-DTO-USU          PIC 9(05)V9(02).
          03 IMP-A-CARGO-PAG      PIC 9(05)V9(02).
          03 COD-CAMPANIA-BCO     PIC X(04).
          03 FILLER                        PIC X(12).
          03 COD-REGISTRO         PIC X(02).
          03 ID-ARCHIVO           PIC X(01).