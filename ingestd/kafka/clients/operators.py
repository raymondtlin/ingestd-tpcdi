import itertools
import re
import json
import avro.schema

from abc import ABC, abstractmethod


class OperatorFactory:
    """
    Factory Object
    """
    def __init__(self):
        self._creators = {}

    def register_format(self, format: str, creator: object):
        """

        :param format: file format as string
        :param creator: Operator object
        :return:
        """
        self._creators[format] = creator

    def get_operator(self, key: str):
        """
        Retrieves instance of registered operator.
        :param key: registered file_format str
        :return: Operator object
        """
        creator = self._creators.get(key)
        if not creator:
            raise ValueError(key)
        return creator


class Operator(ABC):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_format = file_path.split('\\.')[-1]
        self.schema = None

    def create_handle(self):
        with open(self.file_path) as handle:
            yield handle.readlines()

    @abstractmethod
    def parse(self):
        pass

    def get_schema(self, path_to_schema: str):
        with open(path_to_schema) as handle:
            try:
                j = json.dumps(handle)
            except BaseException:
                print("Unable to serialize schema as json")
            if j is not None:
                try:
                    self.schema = avro.schema.parse(j)
                except BaseException:
                    print("Unable to convert from json to avro schema")


class XMLOperator(Operator):
    def __init__(self, file_path):
        super().__init__(file_path)

    def parse(self):
        pass


class DelimitedOperator(Operator):
    """
    Operator to handle delimited files
    """
    def __init__(self, file_path, delimiter):
        super().__init__(file_path)
        self.delimiter = delimiter

    def parse(self):
        for record in self.create_handle():
            for itm in record.split(self.delimiter):
                yield itm


class FixedWidthOperator(Operator):
    """
    Operator to handle fixed-width format files
    """
    def __init__(self, file_path=None, field_widths_lookup=None):
        super().__init__(file_path)
        self.field_width_lookup = field_widths_lookup or {}

    def classify_record(self, record: list(str)):
        """
        Searches for the 3-character document type in the record
        :param: record: str
        :return: record_type: str # One of ("SEC","CMP","FIN")
        """
        token = re.compile(r'(SEC|CMP|FIN)')
        record_type = re.findall(token, record)[0]

        if record_type in ('SEC', 'FIN', 'CMP'):
            return record_type[0]

        else:
            print('Invalid return value from classify_record method')
            raise ValueError

    def create_parser(self, field_widths: tuple):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in field_widths))
        pads = tuple(fw < 0 for fw in field_widths)
        fields = tuple(itertools.zip_longest(pads, (0, ) + cuts, cuts))[:-1]
        return lambda line: tuple(line[i: j] for pad, i, j in fields if not pad)

    def parse(self):
        try:
            for record in self.create_handle():
                rec_type = self.classify_record(record)
                record_field_widths = self.field_width_lookup.get(rec_type)
                p = self.create_parser(record_field_widths)(record)
                yield rec_type, p
        except BaseException as e:
            print(f"Exception in record parsing {e}")