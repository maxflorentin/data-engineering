import xlrd
import json
import sys
import os
import glob

def xls_to_json():
    path = os.path.dirname(os.path.abspath(__file__))
    list_xls = glob.glob(os.path.join(path, "*.xlsx"))

    for xls in list_xls:
        book = xlrd.open_workbook(os.path.join(path, xls))

        sh1 = book.sheet_by_name("Metadata Tabla")
        sh2 = book.sheet_by_name("Metadata Columnas")

        frame = {
            "objectMetadata": {
                "active": True,
                "governance": {
                    "steward": [
                        "Legajos Responsables Tecnicos separados por ','"
                    ],
                    "level": "Basic|Intermediate|Advanced|Optimized"
                },
                "table": {
                    "name": "nombre de la tabla",
                    "title": sh1.row(rx)[2].value,
                    "schema": "bi_corp_staging|bi_corp_common|bi_corp_business",
                    "source": sh1.row(rx)[1].value,
                    "query": "HQL del ETL",
                    "type": sh1.row(rx)[3].value,
                    "description": sh1.row(rx)[2].value
                },
                "schedule": {
                    "periodicity": sh1.row(rx)[4].value,
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
                "sourceColumns": [{
                    "schema": "source schema",
                    "table": "source table",
                    "column": "source column"
                }],
                "security": "Secreto|Confidencial|Restringido|Interno|Publico"
            } for rx in range(2, sh2.nrows)]

        frame['objectMetadata']['table']['columns'] = columns
        data = json.dumps(frame, indent=4, ensure_ascii=False)
        output = os.path.join(path, '%s_alation_schema.json' % xls[:-5])
        savefile = open(output, 'w', encoding='utf_8')
        savefile.write(data)
        savefile.close()
        print("Completed: %s" % xls[:-5])

    return print("Success! json for %s tables" % len(list_xls))

if __name__ == xls_to_json():
    sys.exit(xls_to_json())

