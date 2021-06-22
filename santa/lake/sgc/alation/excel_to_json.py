import xlrd
import json
import sys
import os
import glob
import ntpath
#print(glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "*.xls")))


def xls_to_json():
    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    path = os.path.dirname(os.path.abspath(__file__))
    list_xls = glob.glob(os.path.join(path, "*.xlsx"))

    for xls in list_xls:
        book = xlrd.open_workbook(os.path.join(path, xls))

        t_name = path_leaf(xls.title()).lower().replace(".xlsx", '')
        t_schema = t_name.split('--')[0]
        j_schema = "bi_corp_staging" if t_schema == 'staging' else "bi_corp_common|bi_corp_business"
        t_name = t_name.split('--')[1]

        sh1 = book.sheet_by_name("Metadata Tabla")
        sh2 = book.sheet_by_name("Metadata Columnas")

        frame = {
            "objectMetadata": {
                "active": True,
                "governance": {
                    "steward": [
                        [sh1.row(rx)[6].value]
                    ],
                    "level": "Basic" if t_schema == 'staging' else "Intermediate|Advanced|Optimized"
                },
                "table": {
                    "name": j_schema+'.'+t_name,
                    "title": sh1.row(rx)[2].value,
                    "schema": j_schema,
                    "source": sh1.row(rx)[1].value,
                    "query": "HQL del ETL",
                    "type": sh1.row(rx)[3].value,
                    "description": sh1.row(rx)[2].value
                },
                "schedule": {
                    "periodicity": 'Daily' if sh1.row(rx)[4].value == 'DIARIO' else sh1.row(rx)[4].value,
                    "loading": {
                        "type": sh1.row(rx)[5].value,
                        "delta": "D+1"
                    }
                }
            } for rx in range(2, sh1.nrows)
        }

        columns = [
            {
                "name": sh2.row(rx)[1].value,
                "title": sh2.row(rx)[1].value,
                "description": sh2.row(rx)[2].value,
                "type": sh2.row(rx)[3].value,
                "personIdentifier": True if sh2.row(rx)[4].value == 'SI' else False,
                "decimalSeparator": sh2.row(rx)[7].value,
                "nullable": True if sh2.row(rx)[6].value == 'SI' else False,
                "length": sh2.row(rx)[5].value,
                "security": "Publico"
            } for rx in range(2, sh2.nrows)]

        frame['objectMetadata']['table']['columns'] = columns
        data = json.dumps(frame, indent=4, ensure_ascii=False)
        output = os.path.join(path, '%s_alation.json' % t_name)
        savefile = open(output, 'w', encoding='utf_8')
        savefile.write(data)
        savefile.close()
        print("Completed: %s" % t_name)

    return print("Success! json for %s tables" % len(list_xls))

if __name__ == xls_to_json():
    sys.exit(xls_to_json())

