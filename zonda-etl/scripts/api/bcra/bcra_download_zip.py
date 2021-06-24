import os, requests, zipfile, io

proxies = {
    "http": "http://proxy.ar.bsch:8080",
    "https": "http://proxy.ar.bsch:8080",
}

URL = "http://www.afip.gob.ar/genericos/cInscripcion/archivos/SINapellidoNombreDenominacion.zip"
filename = "test_afip.zip"

response = requests.get(URL, stream=True, proxies=proxies)

if response.status_code == 200:
    # print('Response: ')
    # print(response.content)

    #The class for reading and writing ZIP files
    zip = zipfile.ZipFile(URL)
    #Extract all members from the archive to the current working directory.
    zip.extractall(path=None, members=None, pwd=None)


    print("URL:") #wd_from,folder
    print(os.path.join("SINapellidoNombreDenominacion", "/utlfile/padr",))

    with open(filename, 'wb') as out:
        out.write(response.content)
else:
    print('Request failed: %d' % response.status_code)