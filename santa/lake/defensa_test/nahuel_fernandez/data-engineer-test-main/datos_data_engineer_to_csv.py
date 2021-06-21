import io

with io.open("datos_data_engineer.tsv","r",encoding='utf-16-le') as f:

    # tomo la primer linea como header
    headers = f.readline().replace("\n","").split("\t")
    rows = []
    rows.append(headers)
    line = f.readline()
    while line <> '':
       
        row = []
        x = 0
        # loopeo por la cantidad de campos que hay en el header - TOIMRPOVE: que pasa si no tiene header?
        while x < len(headers):
            # si es el ultimo campo, leo todo lo que quede de la linea
            if (x == len(headers) - 1):
                value = line
            # si no es el ultimo campo, leo hasta el siguiente \t
            else:    
                index_x = -1
                value = ''
                while index_x < 0:
                    index_x = line.find("\t")
                    if(index_x < 0):
                        value += line
                        line = f.readline()
                    else:
                        value += line[0:index_x]
                        line = line[index_x+1:]
            row.append(value.encode(encoding='UTF-8',errors='strict').replace("\n",""))
            x += 1
        
        line = f.readline()
        # agrego la nueva fila detectada 
        rows.append(row)

    # escribo el resultado en el csv con encode utf-8
    with io.open("datos_data_engineer.csv","w",encoding="utf-8") as output:
        for row in rows:
            output.write(('|'.join(row)+"\n").decode('utf-8'))