from __future__ import absolute_import

import collections
import operator
import types

from confluent_kafka import avro


class FileSource:
    """
    Abstraction for an opened file.
    """
    def __init__(self, **kwargs):
        """
        :param kwargs:
            file_path          (str): source file path
            schema_path        (str): avro schema path
            key_fields   (list(str)): field as list or list of fields to use as the composite key
            parser            (func): parsing function as defined in ingestd/strategies.py
        """
        self.file_path = kwargs.pop('file_path', None)
        self.schema_path = kwargs.pop('schema_path', None)
        self.schema = self.get_schema(self.schema_path)
        self.fields = list(self.schema.field_map)
        self.key_fields = list(field for field in kwargs.pop('key_fields'))
        self.value_fields = list(field for field in self.fields if field not in self.key_fields)

        if kwargs.get('parser', None):
            self.parse = types.MethodType(kwargs.pop('parser'), self)

        if kwargs.pop('file_format', None):
            self.file_format = kwargs.pop('file_format', None)
        else:
            self.file_format = self.file_path.split('.')[-1].lower()

    @property
    def read(self):
        """
        Creates a stream handler
        """
        with open(self.file_path) as handle:
            yield self.read

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
            except RuntimeError as e:
                print(f"Unable to serialize schema as avro, {e}")

    def set_record_keys(self, key_fields: list = None) -> None:
        """
        Sets the fields to be used as part of the composite key
        """
        if key_fields:
            setattr(self.key_fields, set(field for field in key_fields))

    def set_record_values(self, value_fields: list = None) -> None:
        """
        Sets the fields to be used as values
        """
        if value_fields is not None:
            setattr(self.value_fields, list(value
                                            for value in value_fields
                                            if value not in self.key_fields))
        else:
            setattr(self.value_fields, list(value
                                            for value in self.fields
                                            if value not in self.key_fields))

    def produce_payload(self, specific_flag: bool = False):
        """
        Creates a message which can be sent to a Kafka Producer.
        :param specific_flag: if set to False, yields named tuple of all fields \
                              if set as True, yields a dict with two keys [key, value]
        :return: namedtuple or dict
        """
        # generic/specificPayload is an homage paid to General/SpecificRecord
        # in the Kafka-AvroSerializer java source.

        # genericPayload is a namedtuple of the field names mapped to field values
        genericPayload = collections.namedtuple('Payload_', self.fields)

        # specificPayload is a genericPayload as dict with specified fields in "key" and "value"
        specificPayload = {}

        # By inferring the delimiter, we preclude a ValueError in the call to parseDelimited

        for ntuple in map(genericPayload._make, iter(self.parse())):
            if not specific_flag:
                yield ntuple
            else:
                dct = ntuple._asdict()
                specificPayload['record_keys'] = operator.itemgetter(*self.key_fields)(dct)
                specificPayload['record_values'] = operator.itemgetter(*self.value_fields)(dct)
                specificPayload['record_key_names'] = list(self.key_fields)
                specificPayload['record_value_names'] = list(self.value_fields)

                # If the message consists of a composite key with multiple fields,\
                # we should iterate over them and zip the contents
                if len(specificPayload['record_key_names']) > 1:
                    specificPayload['key'] = dict(
                        {k: v
                         for k, v in zip(specificPayload.pop('record_key_names'),
                                         specificPayload.pop('record_keys'))
                         })
                else:
                    specificPayload['key'] = dict(
                        {specificPayload.pop('record_key_names'): specificPayload.pop('record_keys')
                         })

                specificPayload['value'] = dict(
                    {k: v
                     for k, v in zip(specificPayload.pop('record_value_names'),
                                     specificPayload.pop('record_values'))
                     })

                yield specificPayload
