import itertools
import re
import json
import collections
from confluent_kafka import avro
from abc import ABC, abstractmethod


class OperatorFactory:
    """
    Factory Object
    """
    def __init__(self):
        self._operators = {}

    def register_operator(self, key: str, operator: Operator):
        """

        :param key: identifier String for registered Operator
        :param operator: Operator object
        :return:
        """
        self._operators[format] = operator

    def get_operator(self, key: str):
        """
        Retrieves instance of registered operator.
        :param key: registered file_format str
        :return: Operator object
        """
        operator = self._operators.get(key)
        if not operator:
            raise ValueError(key)
        return operator


class Operator(ABC):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_format = file_path.split('\\.')[-1]
        self.schema = None
        self.record_key = None or []
        self.record_values = None or []

    def create_handle(self):
        with open(self.file_path) as handle:
            yield from handle.readlines()

    @abstractmethod
    def parse(self):
        pass

    def set_schema(self, path_to_schema: str):
        with open(path_to_schema) as handle:
            try:
                self.schema = avro.loads(handle.read())
                print(self.schema)
            except Exception as e:
                print(f"Unable to serialize schema as avro, {e}")

    def set_record_key(self, key_fields=None):
        if key_fields is not None:
            self.record_key = [field for field in key_fields]

    def set_record_value(self, value_fields=None):
        if value_fields is not None:
            self.record_values = [value for value in value_fields if value not in self.record_key]

    def gen_payload(self):
        for parsed_item in self.parse():
            record_dict = {}
            for k, v in zip(dict(self.schema.field_map), parsed_item):
                record_dict[k] = v

    def send_payload(self):
        for message in self.gen_payload():
            payload_key = (message.get(key) for key in self.record_key)
            payload_record = (message.get(value) for value in message.keys()
                              if value not in self.record_key)
            yield payload_key, payload_record


class XMLOperator(Operator):
    def __init__(self, file_path):
        super().__init__(file_path)

    def parse(self):
        pass


class DelimitedOperator(Operator):
    """
    Operator to handle delimited files
    """
    def __init__(self, file_path=None, delimiter=None):
        super().__init__(file_path)
        self._delimiter = delimiter

    def infer_delimiter(self):
        """
        Infer the record delimiter by
        :return:
        """
        non_alnum = collections.deque()
        for char in self.create_handle().__next__():
            if not char.isalnum():
                non_alnum.append(char)
        inferred_delimiter = tuple(collections.Counter(non_alnum))[0]
        self.__setattr__('_delimiter', inferred_delimiter)


    def parse(self):
        if len(self._delimiter) == 0:
            self.infer_delimiter()
            raise Warning("No delimiter specified, inferring delimiter...")

        for _ in self.create_handle():
            items = _.rstrip().split(self._delimiter)
            if len(items) == 0:
                print("Error splitting record into fields")
                raise ValueError
            else:
                yield items




class FixedWidthOperator(Operator):
    """
    Operator to handle fixed-width format files
    """
    def __init__(self, file_path=None, field_widths_lookup=None):
        super().__init__(file_path)
        self.field_width_lookup = field_widths_lookup or {}

    def classify_record(self, record):
        """
        Searches for the 3-character document type in the record
        :param: record: str
        :return: record_type: str # One of ("SEC","CMP","FIN")
        """
        token = re.compile('SEC|CMP|FIN')
        record_type = re.findall(pattern=token,string=record)[0]

        if record_type in ('SEC', 'FIN', 'CMP'):
            return record_type[0]

        else:
            print('Invalid return value from classify_record method')
            raise ValueError

    def _create_parser(self, field_widths: tuple):
        cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in field_widths))
        pads = tuple(fw < 0 for fw in field_widths)
        fields = tuple(itertools.zip_longest(pads, (0, ) + cuts, cuts))[:-1]
        return lambda line: tuple(line[i: j] for pad, i, j in fields if not pad)

    def parse(self):
        try:
            for record in self.create_handle():
                rec_type = self.classify_record(record)
                record_field_widths = self.field_width_lookup.get(rec_type)
                p = self._create_parser(record_field_widths)(record)
                yield rec_type, p
        except BaseException as e:
            print(f"Exception in record parsing {e}")