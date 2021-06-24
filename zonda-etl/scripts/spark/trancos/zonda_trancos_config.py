import json
import os

from pyspark.sql.types import StructType, StructField, StringType, BinaryType, BooleanType, DecimalType, DoubleType, \
    FloatType, ByteType, IntegerType, LongType, ShortType


def map_field_type(f_type):
    struct_fields = {
        "string": StringType(),
        "binary": BinaryType(),
        "boolean": BooleanType(),
        "date": StringType(),
        "timestamp": StringType(),
        "decimal": DecimalType(),
        "double": DoubleType(),
        "float": FloatType(),
        "number": StringType(),
        "byte": ByteType(),
        "integer": IntegerType(),
        "long": LongType(),
        "short": ShortType(),
        None: StringType()
    }
    return struct_fields.get(f_type, StringType())


class ConfigFile:

    def generate_schema(self):
        fields = []
        for col in self.columns:
            name = col.get("name")
            data_type = map_field_type(col.get("type"))
            nullable = col.get("null", True)
            field = StructField(name, data_type, nullable)
            fields.append(field)
        return StructType(fields)

    def control_print(self):
        for var in self.__dict__:
            print(var + ': ' + str(self.__dict__[var]))

    def __init__(self, file_path):
        self.file_path = file_path
        if type(file_path) is dict:
            self.config_data = file_path
        else:
            with open(file_path, "r") as f:
                self.config_data = json.loads(f.read().decode("utf-8-sig"))

        self.file_name = self.config_data.get("file")
        if len(self.config_data.get("location")) > 1:
            self.location = []
            tempLocation = self.config_data.get("location")
            self.location.append(os.path.expandvars(tempLocation[0]))
            self.location.append(os.path.expandvars(tempLocation[1]))
        else:
            self.location = []
            tempLocation = self.config_data.get("location")
            self.location.append(os.path.expandvars(tempLocation[0]))
        self.destination = os.path.expandvars(self.config_data.get("destination"))
        self.temp_directory = self.config_data.get("tempDirectory") \
            if self.config_data.get(
            "tempDirectory") is not None else '/santander/bi-corp/temp/trancos/' + self.destination
        self.extension = self.config_data.get("extension") if self.config_data.get("extension") is not None else False
        self.model = self.config_data.get("model") if self.config_data.get("model") is not None else None
        self.infer_schema = True if self.config_data.get("columns") is None else False
        self.delimiter = self.config_data.get("delimiter") if self.config_data.get("delimiter") is not None else False
        self.columnDelimiter = self.config_data.get("columnDelimiter") if self.config_data.get("columnDelimiter") is not None else None
        self.parquet_files = self.config_data.get("parquetFiles") \
            if self.config_data.get("parquetFiles") is not None else 1
        self.compression_rate = float(self.config_data.get("CompressionRate")) \
            if self.config_data.get("CompressionRate") is not None else 0.03
        self.compression = self.config_data.get("compression") \
            if self.config_data.get("compression") is not None else "gzip"
        self.columns = self.config_data.get("columns") if self.config_data.get("columns") is not None else []
        self.trancos = self.config_data.get("Trancos") if self.config_data.get("Trancos") is not None else {}
        self.schema = self.generate_schema()
