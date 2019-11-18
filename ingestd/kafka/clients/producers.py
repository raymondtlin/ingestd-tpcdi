from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer


class ProducerFactory:
    def __init_(self):
        self._producers = {}

    def register_producer(self, key, producer: Producer or AvroProducer):
        self._producers[key] = producer

    def create(self, key, rc):
        producer = self._producers.get(key)
        if not producer:
            raise ValueError(key)
        else:
            return producer(**rc)


import sys
import types
import collections
import itertools
import operator
import re

from confluent_kafka import avro


if sys.version_info[0] > 2:                                                                                 # Python 3
    createBoundMethod = types.MethodType
else:
    def createBoundMethod(func, obj):                                                               # Python 2
        return types.MethodType(func, obj, obj.__class__)


class Operator(object):
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

