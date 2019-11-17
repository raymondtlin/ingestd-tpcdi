import collections
import itertools
import operator
import re

from abc import ABC, abstractmethod

from confluent_kafka import avro


class OperatorFactory:
    """
    Factory Object
    """
    def __init__(self):
        self._operators = {}

    def register_operator(self, key: str, operator: object):
        """

        :param key: identifier String for registered Operator
        :param operator: Operator object
        :return:
        """
        self._operators[key] = operator

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
    def __init__(self,
                 source_path: str = None,
                 schema_path: str = None,
                 message_key: set = None,
                 message_value: set = None):
        self.source_path = source_path
        self.file_format = source_path.split('\\.')[-1]
        self.schema = self.get_schema(schema_path)
        self.fields = set(self.schema.field_map)
        self.record_keys = self.set_record_keys(message_key)
        self.record_values = self.set_record_values(message_value)

    def create_handle(self):
        with open(self.source_path) as handle:
            yield from handle.readlines()

    @abstractmethod
    def parse(self):
        pass

    @staticmethod
    def get_schema(path_to_schema: str = None) -> object:
        with open(path_to_schema) as handle:
            try:
                return avro.loads(handle.read())
            except Exception as e:
                print(f"Unable to serialize schema as avro, {e}")

    @staticmethod
    def set_record_keys(key_fields: list = None) -> set:
        if key_fields is not None:
            return set(field for field in key_fields)

    def set_record_values(self, value_fields: list = None) -> set:
        if value_fields is not None:
            return set(value for value in value_fields if value not in self.record_keys)
        else:
            return set(value for value in self.fields if value not in self.record_keys)

    # def produce_general_payload(self):
    #     """
    #     Lazy-generates records as namedtuples
    #     :return: namedtuple record
    #     """
    #     Payload = collections.namedtuple('Payload_', self.fields)
    #     for nt in map(Payload._make, iter(self.parse())):
    #         yield nt
    #
    # def produce_specific_payload(self):
    #
    #     payload = collections.defaultdict(list)
    #
    #     for message in self.produce_general_payload():
    #         msg_dct = message._asdict
    #
    #         payload['record_key'] = operator.itemgetter(*self.record_key)(msg_dct)
    #         payload['record_val'] = operator.itemgetter(*self.record_values)(msg_dct)
    #         payload['key_fields'] = list(*self.record_key)
    #         payload['value_fields'] = list(*self.record_values)
    #         yield payload

    def produce_payload(self, specific_flag: bool = False):
        genericPayload = collections.namedtuple('Payload_', self.fields)

        specificPayload = {}

        for ntuple in map(genericPayload._make, iter(self.parse())):
            if specific_flag:
                yield ntuple
            else:
                dct = ntuple._asdict()
                specificPayload['record_key'] = operator.itemgetter(*self.record_keys)(dct)
                specificPayload['record_value'] = operator.itemgetter(*self.record_values)(dct)
                specificPayload['record_key_fields'] = set(*self.record_keys)
                specificPayload['value_key_fields'] = set(*self.record_values)

                yield specificPayload


class XMLOperator(Operator):
    def __init__(self, source_path: str = None):
        super().__init__(source_path)

    def parse(self):
        pass


class DelimitedOperator(Operator):
    """
    Operator to handle delimited files
    """
    def __init__(self,
                 source_path: str = None,
                 schema_path: str = None,
                 message_keys: set = None,
                 message_values: set = None):
        super().__init__(source_path, schema_path, message_keys, message_values)
        self._delimiter = self.infer_delimiter() or self.set_delimiter()

    def set_delimiter(self, delimiter: str = None):
        self.__setattr__("_delimiter", delimiter)
        print(f"Set delimiter as {self._delimiter}")

    def infer_delimiter(self):
        """
        Infer the record delimiter by
        :return: delimiter string
        """
        non_alnum = collections.deque()
        for char in self.create_handle().__next__():
            if not char.isalnum():
                non_alnum.append(char)
        inferred_delimiter = tuple(collections.Counter(non_alnum))[0]

        if len(inferred_delimiter) != 1:
            print(f"Inferred Delimiter is {inferred_delimiter}.\
                    Please set delimiter using the class method")
            raise ValueError
        else:
            return inferred_delimiter

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
    def __init__(self, source_path: str = None, field_widths_lookup=None):
        super().__init__(source_path)
        self.field_width_lookup = field_widths_lookup or {}

    def classify_record(self, record):
        """
        Searches for the 3-character document type in the record
        :param: record: str
        :return: record_type: str # One of ("SEC","CMP","FIN")
        """
        token = re.compile('SEC|CMP|FIN')
        record_type = re.findall(pattern=token, string=record)[0]

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
