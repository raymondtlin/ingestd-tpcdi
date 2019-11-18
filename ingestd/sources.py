from __future__ import absolute_import

import collections
import operator
import types

from confluent_kafka import avro


class FileSource(object):
    def __init__(self, **kwargs):
        self.file_path = kwargs.pop('file_path', None)
        self.schema_path = kwargs.pop('schema_path', None)
        self.schema = self.get_schema(self.schema_path)
        self.fields = set(self.schema.field_map)
        self.key_fields = set(field for field in kwargs.pop('key_fields'))
        self.value_fields = set(field for field in self.fields if field not in self.key_fields)

        if kwargs.get('parser', None):
            self.parse = types.MethodType(kwargs.pop('parser'), self)

        if kwargs.pop('file_format', None):
            self.file_format = kwargs.pop('file_format', None)
        else:
            self.file_format = self.file_path.split('.')[-1].lower()

    def stream(self):
        """
        Creates file handle, generates lines
        :return: list(str)
        """
        with open(self.file_path) as handle:
            yield from handle.readlines()

    def parse(self):
        """
        Abstract strategy to be implemented by Source instances.
        """
        error_message = f'{self.__class__.__name__} implement a parse method'
        raise NotImplementedError(error_message)

    @staticmethod
    def get_schema(path_to_schema: str = None) -> object:
        with open(path_to_schema) as handle:
            try:
                return avro.loads(handle.read())
            except Exception as e:
                print(f"Unable to serialize schema as avro, {e}")

    def set_record_keys(self, key_fields: list = None) -> None:
        if key_fields:
            setattr(self.key_fields, set(field for field in key_fields))

    def set_record_values(self, value_fields: list = None) -> None:
        if value_fields is not None:
            setattr(self.value_fields, set(value for value in value_fields if value not in self.key_fields))
        else:
            setattr(self.value_fields,  set(value for value in self.fields if value not in self.key_fields))

    def produce_payload(self, specific_flag: bool = False):
        """
        Creates a message which can be sent to a Kafka Producer.
        :param specific_flag: if set to False, yields named tuple of all fields \
                              if set as True, yields dict with fields seperated by specified schema
        :return: namedtuple or dict
        """
        genericPayload = collections.namedtuple('Payload_', self.fields)
        specificPayload = {}

        for ntuple in map(genericPayload._make, iter(self.parse())):
            if not specific_flag:
                yield ntuple
            else:
                dct = ntuple._asdict()
                specificPayload['record_key'] = operator.itemgetter(*self.key_fields)(dct)
                specificPayload['record_value'] = operator.itemgetter(*self.value_fields)(dct)
                specificPayload['record_key_fields'] = set(self.key_fields)
                specificPayload['value_key_fields'] = set(self.value_fields)

                yield specificPayload