import os
import glob
import ntpath
import xlrd
import json

path = os.path.dirname(os.path.abspath(__file__))
list_xls = glob.glob(os.path.join(path, "*.xlsx"))
# print(path, list_xls)
print('\nLooking for xlsx files in {} >\nFind {} files.\n'.format(path,len(list_xls)))

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def get_json():
    for xls in list_xls:
    # TABLE VARIABLES
        t_name = path_leaf(xls.title()).lower().replace(".xlsx", '')
        t_schema = t_name.split('--')[0]
        j_schema = "bi_corp_staging" if t_schema == 'staging' else "bi_corp_common|bi_corp_business"
        t_name = t_name.split('--')[1]

    # PARSED
        book = xlrd.open_workbook(os.path.join(path, xls))
        sh1 = book.sheet_by_name("Metadata Tabla")
        sh2 = book.sheet_by_name("Metadata Columnas")

    # METADATA COLUMNS
        dict_columns = []
        for rx in range(2, sh2.nrows):
            col = {"name": sh2.row(rx)[1].value,
                    "title": sh2.row(rx)[1].value,
                    "description": sh2.row(rx)[2].value,
                    "type": sh2.row(rx)[3].value,
                    "personIdentifier": True if sh2.row(rx)[4].value == 'SI' else False,
                    "decimalSeparator": 'NA' if sh2.row(rx)[7].value == '' else sh2.row(rx)[7].value,
                    "nullable": True if sh2.row(rx)[6].value == 'SI' else False,
                    "length": sh2.row(rx)[5].value,
                    "security": "Secreto|Confidencial|Restringido|Interno|Publico"}
            dict_columns.append(col)

    # CREATE JSON
        table = {"objectMetadata":
                {"active": True,
                "governance": {"steward": [sh1.row(2)[6].value],
                "level": "Basic" if t_schema == 'staging' else "Intermediate|Advanced|Optimized"},
                "table": {"name": j_schema+'.'+t_name,
                            "title": sh1.row(2)[2].value,
                            "schema": j_schema,
                            "source": sh1.row(2)[1].value,
                            "query": "HQL del ETL",
                            "type": sh1.row(2)[3].value,
                            "description": sh1.row(2)[2].value },
                            "columns": [dict_columns],
                "schedule": {"periodicity": 'Daily' if sh1.row(2)[4].value == 'DIARIO' else sh1.row(2)[4].value,
                                "loading": {"type": sh1.row(2)[5].value,
                                            "delta": "D+1"}}}}

    # GENERATE FILES
    data = json.dumps(table, indent=4, ensure_ascii=False)
    output = os.path.join(path, '{}_alation_schema.json'.format(t_name))
    try:
        savefile = open(output, 'w', encoding='utf_8')
        savefile.write(data)
        savefile.close()
        return print("\nCreate {} json file\n".format(len(list_xls)))
    except:
        print('Check xlsx files! Not generated json files\n')

if __name__ == get_json():
    sys.exit(get_json())

