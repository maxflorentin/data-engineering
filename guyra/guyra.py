# -*- coding: utf-8 -*- 
import json 
import os 
from datetime import datetime 
 

from pyspark.sql.types import StructType, StructField, StringType, BinaryType, BooleanType, DecimalType, DoubleType, \ 
    FloatType, ByteType, IntegerType, LongType, ShortType 
 

try: 
    from pyzonda.utils import CobolDataField 
except ImportError: 
    print('Pyzonda Utils not found') 
 

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
 

def set_partitions(self): 
        partition_columns = [] 
        columns = self.columns + self.mirror_columns 
        dynamic_partitions = [] 
        for column in columns: 
            column_name = column.get("name") 
            column_partition = column.get("partitionColumn") 
            column_part_order = column.get("partitionOrder") 
            if column_partition: 
                col_tuple = (column_part_order, str(column_name)) 
                partition_columns.append(col_tuple) 
                dynamic_partitions.append(column_name) 
 

# Variables to avoid unnecessary DF scans 
        avoid_scan_flag = True if len(partition_columns) == 0 else False 
        static_partitions = {} 
 

for column in self.extra_columns: 
            column_name = column.get("name") 
            column_partition = column.get("partitionColumn") 
            column_part_order = column.get("partitionOrder") 
            column_value = os.path.expandvars(column.get("value")) 
            if column_partition: 
                col_tuple = (column_part_order, str(column_name)) 
                partition_columns.append(col_tuple) 
                static_partitions[column_name] = column_value 
        partition_columns = sorted(partition_columns) 
 

partition = [] 
        for column in partition_columns: 
            partition.append(column[1]) 
        return partition, avoid_scan_flag, static_partitions, dynamic_partitions 
 

def get_names_widths_compressed(self): 
        names = [] 
        widths = [] 
        comp_columns = [] 
        if self.cobol_flag: 
            for column in self.columns: 
                cobol_column = CobolDataField(column.get("name"), column.get("type")) 
                names.append(cobol_column.name) 
                widths.append(int(cobol_column.long)) 
                if cobol_column.is_packed: 
                    comp_columns.append({"name": cobol_column.name, "cobol_type": column.get("type")}) 
                column["type"] = cobol_column.type 
        else: 
            for column in self.columns: 
                names.append(column.get("name")) 
                widths.append(column.get("width")) 
        row_length = sum(widths) 
        return names, widths, comp_columns, row_length 
 

def generate_schema(self): 
        fields = [] 
        for col in self.columns: 
            name = col.get("name") 
            data_type = map_field_type(col.get("type")) 
            nullable = col.get("null", True) 
            field = StructField(name, data_type, nullable) 
            fields.append(field) 
        return StructType(fields) 
 

def generate_extra_columns(self): 
        extra_fields = [] 
        for col in self.extra_columns: 
            extra_tuple = { 
                "name": col.get("name"), 
                "type": map_field_type(col.get("type", "string")), 
                "null": col.get("null", True), 
                "value": os.path.expandvars(col.get("value", "")) 
            } 
            extra_fields.append(extra_tuple) 
        return extra_fields 
 

def get_data_columns(self): 
        data_columns = [] 
        for col in self.columns: 
            if col.get("type") == "data": 
                data_columns.append( 
                    { 
                        "name": col.get("name"), 
                        "dataDelimiter": col.get("dataDelimiter"), 
                        "internalColumns": col.get("internalColumns") 
                    } 
                ) 
        return data_columns 
 

def get_dates(self): 
        date_columns = [] 
        for col in self.columns: 
            if col.get("type") == 'date': 
                date_dict = {"name": col.get("name"), "dateFormat": col.get("dateFormat", "yyyy-MM-dd")} 
                date_columns.append(date_dict) 
 

mirror_date_columns = [] 
        for col in self.mirror_columns: 
            if col.get("type") == 'date': 
                date_dict = {"name": col.get("name"), "dateFormat": col.get("dateFormat", "yyyy-MM-dd")} 
                mirror_date_columns.append(date_dict) 
        return date_columns, mirror_date_columns 
 

def get_timestamps(self): 
        timestamp_columns = [] 
        for col in self.columns: 
            if col.get("type") == 'timestamp': 
                timestamp_tuple = {"name": col.get("name"), "dateFormat": col.get("dateFormat", "yyyy-MM-dd hh:mm:ss")} 
                timestamp_columns.append(timestamp_tuple) 
        return timestamp_columns 
 

def get_create_table_params(self): 
        ct_flag = False 
        table_name = '' 
        db_name = '' 
        if self.config_data.get("createTable"): 
            db_name = self.config_data.get("createTable").split('.')[0] 
            try: 
                table_name = self.config_data.get("createTable").split('.')[1] 
            except IndexError: 
                raise ValueError('createTable option is wrongly set, the right pattern is <db_name.table_name>') 
            try: 
                if self.config_data.get("createTable").split('.')[2]: 
                    raise ValueError('createTable option is wrongly set, the right pattern is <db_name.table_name>') 
            except IndexError: 
                pass 
            ct_flag = True 
        return ct_flag, table_name, db_name 
 

def get_threshold(self): 
        if self.config_data.get("threshold"): 
            try: 
                threshold = float(self.config_data.get("threshold")) 
            except ValueError: 
                raise ValueError('threshold option is not a float.') 
            if 0 <= threshold <= 1: 
                return threshold 
            else: 
                raise ValueError('threshold option is not between 0 and 1.') 
        else: 
            return 0.0 
 

def set_partition_string(self): 
        add_partition_flag = '' 
        add_partition_string = '' 
        if self.ct_flag: 
            if self.config_data.get("addPartition") == False: 
                add_partition_flag = False 
            else: 
                add_partition_flag = True 
                if len(self.partitions) != len(self.static_partitions): 
                    raise ValueError('addPartition option is True but not all partitions are static, set False.') 
                for partition in self.static_partitions: 
                    add_partition_string += '{column} = \'{value}\','.format(column=partition, 
                                                                             value=self.static_partitions.get(partition) 
                                                                             ) 
                add_partition_string = add_partition_string[:-1] 
        else: 
            add_partition_flag = False 
        return add_partition_flag, add_partition_string 
 

def control_print(self): 
        for var in sorted(self.__dict__.keys()): 
            print(var + ': ' + str(self.__dict__[var])) 
 

def __init__(self, file_path): 
        self.file_path = file_path 
        if type(file_path) is dict: 
            self.config_data = file_path 
        else: 
            with open(file_path, "r") as f: 
                self.config_data = json.loads(f.read().decode("utf-8-sig")) 
 

self.file_name = self.config_data.get("file") 
        self.location = os.path.expandvars(self.config_data.get("location")) 
        self.write_mode = self.config_data.get("writeMode") \ 
            if self.config_data.get("writeMode") in ["Append", "ErrorIfExists", "Ignore", "Overwrite"] else "Overwrite" 
        self.destination = os.path.expandvars(self.config_data.get("destination")) 
        self.temp_directory = self.config_data.get("tempDirectory") \ 
            if self.config_data.get( 
            "tempDirectory") is not None else '/santander/bi-corp/temp/guyra/' + self.destination 
        self.extension = self.config_data.get("extension") if self.config_data.get("extension") is not None else False 
        self.copybook = self.config_data.get("copybook") if self.config_data.get("copybook") is not None else False 
        self.header = "true" if self.config_data.get("header") else "false" 
        self.infer_schema = True if self.config_data.get("columns") is None else False 
        self.cobol_flag = False if self.config_data.get("cobolTypes") is None else True 
        self.delimiter = self.config_data.get("delimiter") if self.config_data.get("delimiter") is not None else False 
        self.fixed = False if self.delimiter else True 
        self.parquet_size = float(self.config_data.get("maxParquetSize") * 1024 * 1024) \ 
            if self.config_data.get("maxParquetSize") is not None else float(128 * 1024 * 1024) 
        self.parquet_files = self.config_data.get("parquetFiles") \ 
            if self.config_data.get("parquetFiles") is not None else 1 
        self.compression_rate = float(self.config_data.get("CompressionRate")) \ 
            if self.config_data.get("CompressionRate") is not None else 0.03 
        self.compression = self.config_data.get("compression") \ 
            if self.config_data.get("compression") is not None else "gzip" 
        self.columns = self.config_data.get("columns") if self.config_data.get("columns") is not None else [] 
        if self.fixed: 
            self.column_names, self.widths, self.compressed_columns, self.row_length = self.get_names_widths_compressed() 
        self.compressed_columns_flag = True if (self.fixed and (len(self.compressed_columns) > 0)) else False 
        self.extra_columns = self.config_data.get("extraColumns") \ 
            if self.config_data.get("extraColumns") is not None else [] 
        self.extra_columns_dict = self.generate_extra_columns() if len(self.extra_columns) != 0 else None 
        self.data_columns = self.get_data_columns() 
        self.mirror_columns = self.config_data.get("mirrorColumns") \ 
            if self.config_data.get("mirrorColumns") is not None else [] 
        self.partitions, self.avoid_scan_flag, self.static_partitions, self.dynamic_partitions = self.set_partitions() 
        self.partition_flag = True if len(self.partitions) > 0 else False 
        self.schema = self.generate_schema() 
        self.dates, self.mirror_dates = self.get_dates() 
        self.timestamps = self.get_timestamps() 
        self.execution_timestamp = True if self.config_data.get("executionTimestamp") else False 
        self.guyra_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") 
        self.disregard_dynamic = True if self.config_data.get("disregardDynamic") else False 
        self.create_empty_partitions = True if self.config_data.get("createEmptyPartitions") else False 
        if self.create_empty_partitions and (len(self.partitions) != len(self.static_partitions)): 
            raise ValueError('createEmptyPartitions is set to True but not all partitions are static.') 
        self.ct_flag, self.table_name, self.db_name = self.get_create_table_params() 
        self.add_partitions_flag, self.add_partition_string = self.set_partition_string() 
        self.threshold = self.get_threshold() 