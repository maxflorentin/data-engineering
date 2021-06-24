from zipfile import ZipFile 

class Zip_File:

    def unzip(UrlR,wd_from):      

        with ZipFile(UrlR) as zip: 
            #Extract to folder        
            zip.extractall(wd_from) 
            print('Done!') 

