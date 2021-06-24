import os
import shutil
import requests
import tabula 

class File():

    def Rename(path_f, folder, name_file,date):   

        for afile in os.listdir(os.path.join(path_f,folder)):

            filename, file_extension = os.path.splitext(afile)

            if file_extension == '.tmp':
                os.rename(os.path.join(path_f, folder, afile),  os.path.join(path_f, folder, name_file + '_' + date + '.txt'))
            elif file_extension == '.pdf':
                os.rename(os.path.join(path_f, folder, afile),  os.path.join(path_f, folder, name_file + '_' + date + '.pdf'))
            elif file_extension == '.csv':
                os.rename(os.path.join(path_f, folder, afile),  os.path.join(path_f, folder, name_file + '_' + date + '.csv'))   

    def Move(path_f, path_d, folder): 

        for afile in os.listdir(os.path.join(path_f,folder)):

            shutil.move(os.path.join(path_f, folder, afile), os.path.join(path_d, afile)) 

    def Delete(path_f, folder):

        for afile in os.listdir(os.path.join(path_f,folder)):           

            os.remove(os.path.join(path_f, folder,afile))            

    def MoveTabula(path_f, path_d, folder, name_file, date): 

        for afile in os.listdir(os.path.join(path_f,folder)):

            IFile = os.path.join(path_f, folder, afile)
            Dfile = os.path.join(path_d + name_file + '_' + date + '.csv') 

            tabula.convert_into(IFile, Dfile, output_format="csv" , stream=True, guess=False, pages='all', area=(62.48, 120, 300, 180))

            os.remove(os.path.join(path_f, folder, afile))

    def DownloadFile(url, fileName, proxies):

        try:
            with open(fileName, "wb") as file:
                UrlR = requests.request('GET', url, proxies=proxies)
                if UrlR.status_code == 200:
                    file.write(UrlR.content)                      
        except requests.RequestException as e:
            print(e)