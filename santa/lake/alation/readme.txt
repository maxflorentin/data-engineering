@aquí Chicos, les comparto el archivo alation_schema.json que es la estructura final que se necesita armar 
a partir del excel compartido por @Mauro Rodriguez @Mauro Gonzalez hace unas semanas para enviar la información 
de metadatado y lineage a Alation. 
La idea es que este json este dentro de la carpetita donde tienen la ingesta de la tabla. 
Tambien les comparto un .py que toma el excel devuelto por el aplicativo origen y lo convierte en un json (adjunto un excel de ejemplo). 
OJO que esto no significa que no tengan que tocar a mano el json, ya que hay algunos campos como la taxonomia de seguridad del campo, 
que los tenemos que completar nosotros de acuerdo a si es de acceso publico o confidencial. 
Tambien les recomiendo leer el siguiente articulo (estara sujeto a modificaciones, pero da un vistazo general) https://alation.ar.bsch/article/141/data-lake-management-best-practices donde se explican que son cada uno de los campos a completar 
y sus valores posibles. 
El script que actualiza la data aun no va a estar productivo ya que le voy a pulir algunas cuestiones pero ustedes ya pueden ir armando los json y dejandolos es un respectiva carpeta dentro del repositorio. Por cualquier duda me avisan.