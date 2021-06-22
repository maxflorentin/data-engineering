import glob
import ntpath
import os
import json
import sys
import xlsxwriter

def get_templateit():
    
    path = os.path.dirname(os.path.abspath(__file__))

    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    # PATH SAMPLE XLSX
    sample_template = glob.glob(os.path.join(path, "sample_template.xlsx"))
    print(sample_template)

    # PATH FILES JSON
    list_json = glob.glob(os.path.join(path, "*.json"))  
    try:
        for json in list_json:
            print(json)
    except:
        print("#############################################\nError al leer archivo\n#############################################")
    

if __name__ == get_templateit():
    sys.exit(get_templateit())

'''
path_files = 'C:\\Users\\a309423\\Documents\\zonda-etl\\scripts\\shared\\init_common\\config\\'  # ZONDA_DIR + "/repositories/zonda-etl/scripts/shared/init_common/config/"
for filename in os.listdir(path_files):
	if filename.endswith(".json"):
		json_file = path_files + filename
		print(json_file)
	try:
		with open(json_file, 'r') as f:
			zonda_dependencies_config = json.load(f)
			print("Json Config Readed: "+zonda_dependencies_config["name"])
	except:
		print("#############################################")
		print("Error al leer archivo")
		print("#############################################")
	continue
'''