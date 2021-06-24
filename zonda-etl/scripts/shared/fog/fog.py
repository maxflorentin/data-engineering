#!/usr/bin/python3

import json
import os
import re
import sys
import math
import hashlib
from pathlib import Path
from array import array

CHECKPOINT_READ_LINES = 100000


class CobolDataField(object):
    DATA_TYPES_SEP = '-+|V+|\.+'
    DECIMAL_SEP = b','

    def __init__(self, name, data_type, is_sensible):
        self.name = re.sub('-+|\s+|\.+', '_', name).lower()
        self.data_type = data_type.upper()
        self.type = 'NA'
        self.is_signed = self.data_type.startswith('S')
        self.is_packed = 'COMP-3' in self.data_type
        self.is_sensible = is_sensible or False

        # implicit decimals
        self.has_implicit_decimal = 'V' in self.data_type
        decimal_part = self.data_type.split('V')[1] if self.has_implicit_decimal else ''

        if decimal_part:
            if re.match('^9$|^99+$', decimal_part):
                self.decimal_places = decimal_part.count('9')
            else:
                r = re.match('9\((\d*)\)', decimal_part)
                if r:
                    self.decimal_places = int(r.group(1) or r.group(2))
                else:
                    self.decimal_places = 0
        else:
            self.decimal_places = 0

        self.unpacked_long = self._get_long(self.data_type if not self.is_packed else self.data_type[:self.data_type.find('COMP-3')].strip())
        self.long = self.unpacked_long if not self.is_packed else math.ceil((self.unpacked_long + 1) / 2)

    def convert(self, value, keep_length=True, unpack=True):
        """
        Convert value
        :param unpack:
        :param keep_length:
        :param value: value to be converted
        :return:
        """
        # anonymize
        if self.is_sensible:
            value = self._anonymize_value(value=value, length=self.long)

        if self.type == 'number':
            # unpack number
            if self.is_packed and unpack:
                value = self._unpack_number(value)

                # decimal places
                if self.decimal_places > 0:
                    if not keep_length:
                        if self.has_implicit_decimal:
                            if (len(value) - 1) > self.decimal_places:
                                value = value[:(len(value) - self.decimal_places)] + self.DECIMAL_SEP + value[(len(value) - self.decimal_places):]
                    else:
                        value = value.strip().zfill(self.unpacked_long + 1)

                else:
                    if keep_length:
                        value = value.strip().zfill(self.unpacked_long + 1)
        else:
            # strip value
            if not keep_length:
                value = value.strip()

        return value

    @staticmethod
    def _unpack_number(value):
        """
        Unpack a COMP-3 number
        :param value:
        :return:
        """
        a = array('B', value)
        if not a:
            return ''
        v = float(0)

        # for all but last digit (half byte)
        for i in a[:-1]:
            v = (v * 100) + (((i & 0xf0) >> 4) * 10) + (i & 0xf)

        # last digit
        i = a[-1]
        v = (v * 10) + ((i & 0xf0) >> 4)

        # negative/positve check.
        if (i & 0xf) == 0xd:
            v = -v

        return (str(int(v)) if v < 0 else '+' + str(int(v))).encode('UTF-8')

    @staticmethod
    def _anonymize_value(value, length=None):
        """
        Anonymize value
        :param value:
        :param length:
        :return:
        """
        value_hashed = hashlib.sha256(value).hexdigest()
        if length:
            if length > len(value_hashed):
                value_hashed = value_hashed.ljust(length)
            else:
                value_hashed = value_hashed[:length]

        return value_hashed.encode('UTF-8')

    def _get_long(self, types):
        """
        Get data type long in bytes
        :param types: datatype
        :return:
        """
        types = types.upper()
        types_splitted = re.split(self.DATA_TYPES_SEP, types)

        if len(types_splitted) == 1:
            # empty string
            if types_splitted[0] == '':
                self.type = 'string'
                return 1

            # string by extension
            if re.match('^X$|^XX+$', types_splitted[0]):
                self.type = 'string'
                return types_splitted[0].count('X')

            # string by compression
            r = re.match('X\((\d*)\)', types_splitted[0])
            if r:
                self.type = 'string'
                return int(r.group(1))

            # number by extension
            if re.match('^S9$|^S99+$|^9$|^99+$', types_splitted[0]):
                long = types_splitted[0].count('9')
                # if types_splitted[0].startswith('S'):
                #     long += 1
                self.type = 'number'
                return types_splitted[0].count('9')

            # number by compression
            r = re.match('9\((\d*)\)|S9\((\d*)\)', types_splitted[0])
            if r:
                self.type = 'number'
                return int(r.group(1) or r.group(2))
        else:
            tail_type = types[len(types_splitted[0]) + 1:]

            head_long = self._get_long(types_splitted[0])
            tail_long = self._get_long(tail_type)
            extra_long = 0 if types[len(types_splitted[0])] == 'V' else 1

            return head_long + tail_long + extra_long

    def __repr__(self):
        """
        String representation
        :return:
        """
        return "DataField(name='{}', " \
               "data_type='{}', " \
               "type='{}', " \
               "long={}, " \
               "unpacked_long={}, " \
               "signed={}, " \
               "is_packed={}, " \
               "is_sensible={}, " \
               "has_implicit_decimal={}, " \
               "decimal_places={})".format(self.name,
                                           self.data_type,
                                           self.type,
                                           self.long,
                                           self.unpacked_long,
                                           self.is_signed,
                                           self.is_packed,
                                           self.is_sensible,
                                           self.has_implicit_decimal,
                                           self.decimal_places)


class Fog(object):
    def __init__(self, copy):
        self.copy = copy
        self.fields = []
        self.files = []
        self.separator = b''
        self.max_lines = 0
        self._read()

    def _read(self):
        fields = []
        with open(self.copy) as json_file:
            data = json.load(json_file)

        self.files = data.get('files')
        self.max_lines = data.get('maxLines', 0)
        self.separator = data.get('separator').encode('UTF-8') if data.get('separator') else b''

        if data:
            tmp_fields = data.get('schema', {}).get('fields')
            for e in tmp_fields:
                new_field = CobolDataField(name=e.get('name'), data_type=e.get('type'), is_sensible=e.get('is_sensible'))
                fields.append(new_field)

        self.fields = fields


if __name__ == '__main__':
    # args
    if len(sys.argv) <= 1:
        raise Exception('Missing argument. Please provide the configuration file as first arg of the program')

    a = Fog(copy=sys.argv[1])

    # read files
    for source_file in a.files:
        source_filename = Path(source_file).name
        source_filename_splitted = source_filename.split('.')
        destination_filename = (source_filename_splitted[0] + '_SAFE' + '.' + source_filename_splitted[1]) if len(source_filename_splitted) >= 2 else source_filename + '_SAFE'
        destination_file = str(Path.joinpath(Path(source_file).parent, destination_filename))
        counter = 0
        max_lines = a.max_lines
        lower_limit = 0
        print("\nProcessing file {}...".format(source_file))
        with open(file=source_file, mode='rb') as f_input, open(file=destination_file, mode='wb+') as f_output:
            line = f_input.readline()
            # print(line)
            # print("\n")

            while line:
                # counters
                counter += 1
                if max_lines > 0:
                    if counter > max_lines:
                        counter -= 1
                        break

                lower_limit = 0
                upper_limit = 0
                new_line_splitted = []

                for field in a.fields:
                    upper_limit += field.long
                    raw_value = line[lower_limit:upper_limit]
                    value = field.convert(value=raw_value, keep_length=True, unpack=True)
                    new_line_splitted.append(value)

                    # print("{}: {{value: {}, pos: {}, unpacked_long: {}, long_packed: {}, pic: {}}}".format(field.name, value, lower_limit + 1, field.unpacked_long, field.long, field.data_type))
                    lower_limit += field.long

                # write new line
                # print(new_line_splitted)
                new_line = a.separator.join(new_line_splitted)
                f_output.write(new_line)

                # read new line
                line = f_input.readline()
                # print('-------------------------------------------------------')
                if (counter % CHECKPOINT_READ_LINES) == 0:
                    # break
                    print('Lines processed -> {}'.format(str(counter)))

        if (counter % CHECKPOINT_READ_LINES) != 0:
            print('Lines processed -> {}'.format(str(counter).ljust(10)))
    print('\nProcess completed')
