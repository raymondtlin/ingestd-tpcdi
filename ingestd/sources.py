from __future__ import absolute_import

import collections
import itertools
import operator
import re
import sys
import types

from confluent_kafka import avro

if sys.version_info[0] > 2:
    createBoundMethod = types.MethodType
else:
    def createBoundMethod(func, obj):
        return types.MethodType(func, obj, obj.__class__)


class Source(object):
    def __init__(self,
                 source_path: str = None,
                 schema_path: str = None,
                 file_format: str = None,
                 message_key: set = None,
                 message_value: set = None):
        self.source_path = source_path
        self.schema = self.get_schema(schema_path)
        self.fields = set(self.schema.field_map)
        self.record_keys = self.set_record_keys(message_key)
        self.record_values = self.set_record_values(message_value)

        if file_format is None:
            self.file_format = source_path.split('.')[-1].lower()
        else:
            self.file_format = file_format

    def stream(self):
        """
        Creates file handle, generates lines
        :return: list(str)
        """
        with open(self.source_path) as handle:
            yield from handle.readlines()

    def parse(self):
        strategy_lku = {'xml': RecordParsingStrategy('parseXML', parseXML(self)),
                        'csv': RecordParsingStrategy('parseCSV', parseDelimited(self, ',')),
                        'txt': RecordParsingStrategy('parseTXT', parseDelimited(self)),
                        'finwire': RecordParsingStrategy('parseFINWIRE', parseFixedWidth(self))}

        strategy = strategy_lku.get(self.file_format)
        yield from strategy.execute()

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


class RecordParsingStrategy:
    def __init__(self, strategyName='default', replacementFn=None):
        self.name = strategyName
        if replacementFn:
            self.execute = createBoundMethod(replacementFn, self)

    def execute(self):
        pass


def parseXML(self):
    if isinstance(self, Source):
        for record in self.stream():
            yield record


def parseDelimited(self, delimiter: str = None):
    if isinstance(self, Source):

        if delimiter is not None:
            _delimiter = delimiter

        else:
            _nonalnum = collections.deque()
            for char in self.stream().__next__():
                if not char.isalnum():
                    _nonalnum.append(char)
            _delimiter = tuple(collections.Counter(_nonalnum))[0]

        for record in self.stream():
            yield record.rstrip().split(sep=_delimiter)
    else:
        raise ValueError(f'Incorrect type, {self} is not an instance of the Operator class.')


def parseFixedWidth(self):

    _fixedwidth_lkup = {}

    def classify(row: str):
        row_type = re.findall(pattern='(SEC|FIN|CMP)', string=row)[0]
        if row_type in {'SEC','FIN', 'CMP'}:
            return row_type

    if isinstance(self, Source):
        for record in self.stream():
            record_type = classify(record)
            _cuts = tuple(cut for cut in itertools.accumulate(abs(fw) for fw in _fixedwidth_lkup.get(record_type)))
            _pads = tuple(fw < 0 for fw in _fixedwidth_lkup.get(record_type))
            _fields = tuple(itertools.zip_longest(_pads, (0, ) + _cuts, _cuts))[:-1]
            fn = lambda line: tuple(line[i: j] for pad, i, j in _fields if not pad)
            yield record_type, fn(record)
    else:
        raise ValueError(f'Incorrect type, {self} is not an instance of the Operator class.')
