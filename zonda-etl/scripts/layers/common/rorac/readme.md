RORAC

Hola si estás leyendo esto es para entender como estamos generando actualmente la data que se diponibiliza para el "take away" a España.

Comenzando 🚀
Las definiciones que nos fueron enviando están en una carpeta Drive que dejo link.

https://drive.google.com/drive/folders/1PXw51vfgm15MyPwDobwRcRsofyFeW6-m?usp=sharing

Dependencias 📋
Las tablas que necesitamos para ejecutar el hql que carga las tablas cmn de activo y pasivo son:

* bi_corp_common.bt_ren_result (mensual)
* bi_corp_staging.rio157_ms0_dt_dwh_generic_result (mensual)
* bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs (diaria)
* bi_corp_common.dim_ren_areanegocioctr (diaria)
* bi_corp_common.dim_ren_jeareanegocioctr  (diaria)
* bi_corp_common.rosetta_nkey_hist (mensual)
* santander_business_risk_ifrs9.ifrs9_tablon (mensual)
* bi_corp_staging.malpe_pedt030 (mensual/diaria)
* bi_corp_bdr.v_baremos_local (?)
* bi_corp_bdr.v_map_baremos_global_local (?)


DAGS: date_from y observaciones a tener en cuenta 🛠️

Para ejecutar por ejemplo Diciembre 2020 en trigger DAG hay que pasarle el valor de cualquier fecha del mes, en este ejemplo puse 2020-12-15, pero ingresando 2020-12-20 obtendriamos el mismo resultado.

https://lxbidusrv01.ar.bsch/admin/airflow/tree?dag_id=LOAD_CMN_RORAC_inputs&num_runs=&root= | {Trigger DAG: 2020-12-15}


La carga de bi_corp_common.bt_ror_input_activo & bi_corp_common.bt_ror_input_pasivo, tiene una serie de pasos en donde va "pegando" los atributos de los contratos informados, el último paso es una carga en staging de la última temporal que incluye todos los contratos, esta tabla de staging no tiene particionado por lo que no persisten los datos de ejecuciones previas.


El DAG que genera los archivos txt es:

https://lxbidusrv01.ar.bsch/admin/airflow/tree?dag_id=RORAC_EXPORT_Tables_Spark&dagRegex= | {Trigger DAG: 2020-12-15}

Este DAG utiliza Spark para generar txt en un path temporal (/user/srvcengineerbi/.sparkStaging/tmp/file.txt) que luego mueve al path /aplicaciones/bi/rorac/{YYYYMMDD}/ para este caso ejemplo el path sería 20201231/.
Los archivos se comprimen en gz.

Mover los archivos generados al path padre ⚙️
El paso siguiente es mover estos archivos al path de donde se llevan los "ficheros" (/aplicaciones/bi/rorac/). se generan en paths con la fecha de la data porque esta pensado para poder ejecutar mes tras mes la historia, una vez que nos den el OK para hacerlo, sin comprometer lso envíos.



Testigos 📦
Los testigos están en /aplicaciones/bi/rorac/TFILES/  copiarlos manualmente (ó una vez productivo RORAC agregar la task en el DAG) en /aplicaciones/bi/rorac para que se puedan llevar los archivos y luego borrar.



Versionado 📌
Todo está en este repositorio. Con un plugin tipo GitLens pueden ver los cambios rápidamente.

Edit: 

DAG STG (Mis Mensual):

https://lxbidusrv01.ar.bsch/admin/airflow/tree?dag_id=LOAD_STG_RIO157-Rentabilidad-Monthly&dagRegex=STG | {Trigger DAG: 2020-12-15}

DAG CMN (load bt_ren_result):

https://lxbidusrv01.ar.bsch/admin/airflow/tree?dag_id=LOAD_CMN_Rentabilidad_Monthly&dagRegex=CMN |  {Trigger DAG: 2021-01-05}


Autores ✒️

Maximiliano Florentin


Expresiones de Gratitud 🎁
a Maricel Badaracco y Ariel Mourino por las definiciones 📢 y
una 🍺 o un ☕ a Walter Serpentini que me dió una mano con la performance.









⌨️ con ❤️ por Maximiliano Florentin 😊