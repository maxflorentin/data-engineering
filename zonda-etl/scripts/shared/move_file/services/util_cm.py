import os
import shutil

def shutil_copy(source,destination):

    try: 
        shutil.copy(source, destination) 
        print("File {} copied successfully.".format(destination)) 
    
    except shutil.SameFileError: 
        print("Source: {} and destination {} represents the same file.".format(source,destination)) 
  
    except PermissionError: 
        print("Permission denied.") 
    
    except: 
        print("Error occurred while copying file {} - {}".format(source,destination)) 

def shutil_move(source,destination):   
     
    try: 
        shutil.move(source, destination) 
        print("File {} move successfully.".format(destination))     

    except shutil.SameFileError: 
        print("Source: {} and destination {} represents the same file.".format(source,destination)) 
    
    except PermissionError: 
        print("Permission denied.") 
    
    except: 
        print("Error occurred while move file {} - {}".format(source,destination)) 

def CreateDirectories(path):

    dirname = os.path.dirname(path)

    if not os.path.isdir(dirname):
        try:
            os.makedirs(dirname)
            print("Created Directory {} successfully.".format(dirname))    

        except (shutil.Error, OSError):
            raise Error('Unable to make directory: %s' % dirname)

def rename(dirc2,path_org,name_file,date_file,file_extension):    

    try: 
        
        os.rename(os.path.join(dirc2),  os.path.join(path_org,name_file + "_" +  date_file +  file_extension)) 
        print("Rename successfully {} --> {}".format(os.path.join(dirc2),os.path.join(path_org,name_file + "_" +  date_file +  file_extension)))
    
    except (FileNotFoundError, IOError):
        print("Wrong file or file path")

def split_path(path):
    head = os.path.dirname(path)
    tail = os.path.basename(path)
    if head == os.path.dirname(head):
        return [tail]
    return split_path(head) + [tail]