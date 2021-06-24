      ******************************************************************
      *             DESCRIPCION DE LOS CAMPOS DE LA COPY.              *
      ******************************************************************
      *        IBECABYP --> C L I E N T E S   G A R R A                *
      *        MAESTRO DE CLIENTES CON DATOS DE PERSONAS               *
      ******************************************************************
      *                                                                *
      * IBECABYP-PERIODO        --> PERIODO                            *
      * IBECABYP-NUM-PERSONA    --> NUMERO DE PERSONA                  *
      * IBECABYP-MARCA-LO       --> MARCA LOCAL DEL CLIENTE            *
      * IBECABYP-COD-MARCLI     --> MARCA DE CLIENTE                   *
      * IBECABYP-COD-SUBMARCL   --> SUBMARCA DE CLIENTE                *
      * IBECABYP-FEC-ALMARCL    --> FECHA ALTA MARCA CLIENTE           *
      * IBECABYP-FEC-ALTSMCL    --> FECHA ALTA SUBMARCA CLIENTE        *
      * IBECABYP-COD-GESTOR     --> CD. DE GESTOR                     *
      * IBECABYP-COD-SITUIRRE   --> CD. SITUACIN IRREGULAR           *
      * IBECABYP-GRADO-FEVE     --> GRADO FEVE                         *
      * IBECABYP-IMP-RIESGO     --> IMPORTE DE RIESGO                  *
      * IBECABYP-IMP-SITUIRRE   --> IMPORTE EN SITUACIN IRREGULAR     *
      * IBECABYP-MARCA-TOPN     --> MARCA CLIENTE TOPN (S / N)         *
      * IBECABYP-COD-KGL        --> CDIGO KGL                         *
      * IBECABYP-PRIM-APELLIDO  --> PRIMER APELLIDO                    *
      * IBECABYP-NOMBRE         --> NOMBRE                             *
      * IBECABYP-TIPO-PERSONA   --> TIPO DE PERSONA - FSICA/JURDICA  *
      * IBECABYP-TIPO-DOCUMENTO --> TIPO DE DOCUMENTO DE IDENTIDAD     *
      * IBECABYP-NUM-DOCUMENTO  --> NRO. DE DOCUMENTO                  *
      * IBECABYP-COD-ACTIVIDAD  --> CDIGO DE ACTIVIDAD ECONMICA      *
      * IBECABYP-SEGMENTO       --> SEGMENTO AL QUE PERTENECE EL       *
      *                             CLIENTE                            *
      * IBECABYP-SUBSEGMENTO    --> SUBSEGMENTO AL QUE PERTENECE EL    *
      *                             CLIENTE                            *
      * IBECABYP-MARCA-PYME     --> MARCA SEGMENTO PYME                *
      * IBECABYP-UC-BANCA       --> CD.BANCA S/UNID. CTRAL. ACTIVOS   *
      *                             IRREG.                             *
      * IBECABYP-UC-DIVISION    --> CD.DIVIS. S/UNID. CTRAL. ACTIVOS  *
      *                             IRREG.                             *
      * IBECABYP-UC-SUC-ADM     --> SUCUR.ADMIN. S/UNID. CTRAL. ACTIVOS*
      *                             IRREG.                             *
      * IBECABYP-EMPLEADO       --> MARCA DE EMPLEADO                  *
      * IBECABYP-TIPO-SOCIE     --> TIPO DE SOCIEDAD RELACIONADA AL    *
      *                             CLIENTE                            *
      * IBECABYP-DOMICILIO      --> DOMICILIO PRINCIPAL DEL CLIENTE    *
      * IBECABYP-LOCALIDAD      --> LOCALIDAD PRINCIPAL DEL CLIENTE    *
      * IBECABYP-PROVINCIA      --> CD. DE PROVINCIA PRINCIPAL DEL    *
      *                             CLIENTE                            *
      * IBECABYP-PAIS           --> PAS DE RESIDENCIA                 *
      * IBECABYP-CODPOS         --> CD. POSTAL                        *
      * IBECABYP-SEXO           --> SEXO                               *
      * IBECABYP-FEC-ALTACLI    --> FECHA DE ALTA DEL CLIENTE EN       *
      *                             PERSONAS                           *
      * IBECABYP-FEC-NACIM      --> FECHA DE NACIMIENTO DEL CLIENTE    *
      * IBECABYP-TELEFONO       --> NRO. DE TELFONO                   *
      * IBECABYP-ESTADO-CIVIL   --> ESTADO CIVIL                       *
      * IBECABYP-NACIONAL       --> NACIONALIDAD                       *
      * IBECABYP-MON-ING-BRUTOS --> INGRESOS BRUTOS                    *
      * IBECABYP-MON-ING-GPO-FLIA --> INGRESOS GRUPO FAMILIAR          *
      * IBECABYP-MON-GASTOS     --> GASTOS                             *
      * IBECABYP-IMP-INGRESO    --> IMPORTE DE INGRESO                 *
      * IBECABYP-PROFESION      --> PROFESION DEL CLIENTE              *
      * IBECABYP-TIPO-ACTIVIDAD --> TIPO DE ACTIVIDAD                  *
      * IBECABYP-CARGO-EMPRESA  --> CARGO EN LA EMPRESA                *
      * IBECABYP-NIVEL-ESTUDIO  --> NIVEL DE ESTUDIO DEL CLIENTE       *
      * IBECABYP-COD-ENT-PREV   --> CODIGO DE ENTIDAD DE PREVISION     *
      * IBECABYP-RAMO-EMPRESA   --> RAMO DE LA EMPRESA                 *
      * IBECABYP-RESP-IVA       --> RESPONSABILIDAD ANTE IVA           *
      * IBECABYP-RESP-GAN       --> RESPONSABILIDAD ANTE GANANCIAS     *
      * IBECABYP-RESP-ING       --> RESPONSABILIDAD ANTE INGRESOS      *
      * IBECABYP-FECHA-ULT-ACRED--> FECHA DE ULTIMA ACREDITACION DEL   *
      *                             PLAN SUELDO                        *
      * IBECABYP-TIPO-PAQUETE   --> TIPO DE PAQUETE DONDE EL CLIENTE ES*
      *                             TITULAR, EN CASO DE SER TITULAR EN *
      *                             MAS DE UNO, ES EL MEJOR PAQUETE    *
      * IBECABYP-MARCABYP       --> MARCA B&P                         *
      ******************************************************************
           02 IBECABYP-PERIODO                    PIC X(06).
           02 IBECABYP-REG.
               05 IBECABYP-MAESTRO.
                  10 IBECABYP-NUM-PERSONA         PIC X(08).
                  10 IBECABYP-PYC-02              PIC X.
                  10 IBECABYP-MARCA-LO            PIC X(02).
                  10 IBECABYP-PYC-03              PIC X.
                  10 IBECABYP-COD-MARCLI          PIC X(02).
                  10 IBECABYP-PYC-04              PIC X.
                  10 IBECABYP-COD-SUBMARCL        PIC X(02).
                  10 IBECABYP-PYC-05              PIC X.
                  10 IBECABYP-FEC-ALMARCL         PIC X(10).
                  10 IBECABYP-PYC-06              PIC X.
                  10 IBECABYP-FEC-ALTSMCL         PIC X(10).
                  10 IBECABYP-PYC-07              PIC X.
                  10 IBECABYP-COD-GESTOR          PIC X(04).
                  10 IBECABYP-PYC-08              PIC X.
                  10 IBECABYP-COD-SITUIRRE        PIC X(02).
                  10 IBECABYP-PYC-09              PIC X.
                  10 IBECABYP-GRADO-FEVE          PIC X(02).
                  10 IBECABYP-PYC-10              PIC X.
                  10 IBECABYP-IMP-RIESGO          PIC X(19).
                  10 IBECABYP-PYC-11              PIC X.
                  10 IBECABYP-IMP-SITUIRRE        PIC X(19).
                  10 IBECABYP-PYC-12              PIC X.
                  10 IBECABYP-COD-TOPN            PIC X(01).
                  10 IBECABYP-PYC-13              PIC X.
                  10 IBECABYP-COD-KGL             PIC X(08).
                  10 IBECABYP-PYC-14              PIC X.
                  10 IBECABYP-PRIM-APELLIDO       PIC X(40).
                  10 IBECABYP-PYC-15              PIC X.
                  10 IBECABYP-NOMBRE              PIC X(40).
                  10 IBECABYP-PYC-16              PIC X.
                  10 IBECABYP-TIPO-PERSONA        PIC X(01).
                  10 IBECABYP-PYC-17              PIC X.
                  10 IBECABYP-TIPO-DOCUMENTO      PIC X(02).
                  10 IBECABYP-PYC-18              PIC X.
                  10 IBECABYP-NUM-DOCUMENTO       PIC X(11).
                  10 IBECABYP-PYC-19              PIC X.
                  10 IBECABYP-COD-ACTIVIDAD       PIC X(08).
                  10 IBECABYP-PYC-20              PIC X.
                  10 IBECABYP-SEGMENTO            PIC X(03).
                  10 IBECABYP-PYC-21              PIC X.
                  10 IBECABYP-SUBSEGMENTO         PIC X(03).
                  10 IBECABYP-PYC-22              PIC X.
                  10 IBECABYP-MARCA-PYME          PIC X(01).
                  10 IBECABYP-PYC-23              PIC X.
                  10 IBECABYP-UC-BANCA            PIC X(02).
                  10 IBECABYP-PYC-24              PIC X.
                  10 IBECABYP-UC-DIVISION         PIC X(02).
                  10 IBECABYP-PYC-25              PIC X.
                  10 IBECABYP-UC-SUC-ADM          PIC X(04).
                  10 IBECABYP-PYC-26              PIC X.
                  10 IBECABYP-EMPLEADO            PIC X(01).
                  10 IBECABYP-PYC-27              PIC X.
                  10 IBECABYP-TIPO-SOCIE          PIC X(02).
                  10 IBECABYP-PYC-28              PIC X.
                  10 IBECABYP-DOMICILIO           PIC X(50).
                  10 IBECABYP-PYC-29              PIC X.
                  10 IBECABYP-LOCALIDAD           PIC X(30).
                  10 IBECABYP-PYC-30              PIC X.
                  10 IBECABYP-PROVINCIA           PIC X(02).
                  10 IBECABYP-PYC-31              PIC X.
                  10 IBECABYP-PAIS                PIC X(03).
                  10 IBECABYP-PYC-32              PIC X.
                  10 IBECABYP-CODPOS              PIC X(08).
                  10 IBECABYP-PYC-33              PIC X.
                  10 IBECABYP-SEXO                PIC X(01).
                  10 IBECABYP-PYC-34              PIC X.
                  10 IBECABYP-FEC-ALTACLI         PIC X(10).
                  10 IBECABYP-PYC-35              PIC X.
                  10 IBECABYP-FEC-NACIM           PIC X(10).
                  10 IBECABYP-PYC-36              PIC X.
                  10 IBECABYP-TELEFONO            PIC X(20).
                  10 IBECABYP-PYC-37              PIC X.
                  10 IBECABYP-ESTADO-CIVIL        PIC X(01).
                  10 IBECABYP-PYC-38              PIC X.
                  10 IBECABYP-NACIONAL            PIC X(04).
                  10 IBECABYP-PYC-39              PIC X.
               05 IBECABYP-INGRESOS.
                  10 IBECABYP-MON-INGR-BRUTOS     PIC X(17).
                  10 IBECABYP-PYC-40              PIC X.
                  10 IBECABYP-MON-INGR-GPO-FLIA PIC X(17).
                  10 IBECABYP-PYC-41              PIC X.
                  10 IBECABYP-MON-GASTOS          PIC X(17).
                  10 IBECABYP-PYC-42              PIC X.
                  10 IBECABYP-IMP-INGRESO         PIC X(20).
                  10 IBECABYP-PYC-43              PIC X.
               05 IBECABYP-DATOS-LAB.
                  10 IBECABYP-PROFESION           PIC X(02).
                  10 IBECABYP-PYC-44              PIC X.
                  10 IBECABYP-TIPO-ACTIVIDAD      PIC X(04).
                  10 IBECABYP-PYC-45              PIC X.
                  10 IBECABYP-CARGO-EMPRESA       PIC X(02).
                  10 IBECABYP-PYC-46              PIC X.
                  10 IBECABYP-NIVEL-ESTUDIO       PIC X(03).
                  10 IBECABYP-PYC-47              PIC X.
                  10 IBECABYP-COD-ENT-PREV        PIC X(04).
                  10 IBECABYP-PYC-48              PIC X.
                  10 IBECABYP-RAMO-EMPRESA        PIC X(04).
                  10 IBECABYP-PYC-49              PIC X.
               05 IBECABYP-SIT-FISCAL.
                  10 IBECABYP-RESP-IVA            PIC X(02).
                  10 IBECABYP-PYC-50              PIC X.
                  10 IBECABYP-RESP-GAN            PIC X(01).
                  10 IBECABYP-PYC-51              PIC X.
                  10 IBECABYP-RESP-ING            PIC X(03).
                  10 IBECABYP-PYC-52              PIC X.
               05 IBECABYP-PLAN-SUELDO.
                  10 IBECABYP-FECHA-ULT-ACRED     PIC X(10).
                  10 IBECABYP-PYC-53              PIC X.
               05 IBECABYP-PRODUCTO.
                  10 IBECABYP-TIPO-PAQUETE        PIC X(04).
                  10 IBECABYP-PYC-54              PIC X.
               05 IBECABYP-IND-RIESGO             PIC X(6).
               05 IBECABYP-PYC-55                 PIC X.
               05 IBECABYP-CANAL                  PIC X(04).
               05 IBECABYP-PYC-56                 PIC X.

      ******************************************************************
      *                         LONGITUD 530                           *
      ******************************************************************
