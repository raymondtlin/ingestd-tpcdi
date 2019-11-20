from __future__ import absolute_import
import os
from glob import glob
from ingestd.sources import FileSource
from confluent_kafka import avro
from ingestd.strategies import parseDelimited

txt_files = [file for file in glob('data/*txt')]

dct = {}
key_schemas, value_schemas = [], []

for file in txt_files:
    dct.update(dict({file.split('/')[-1].split('.')[0]: file}))

for key in dct.keys():
    with open(f'ingestd/avro/schemas/key/{key}Key.avsc') as f:
        key_schemas.append(avro.loads(f.read()))
    with open(f'ingestd/avro/schemas/value/{key}Value.avsc') as f:
        value_schemas.append(avro.loads(f.read()))

for (k, v), key_fields, value_fields in zip(dct.items(), key_schemas, value_schemas):
    dct.update(dict({k: {"path": v, "key_fields": [field.get('name') for field in key_fields.to_json().get('fields')],
                         "value_fields": [field.get('name') for field in value_fields.to_json().get('fields')]}}))


CONFIG = {"bootstrap.servers": os.getenv('CCLOUDBROKERS'),
          "sasl.mechanisms": "PLAIN",
          "security.protocol": "SASL_SSL",
          "sasl.username": os.getenv('CCLOUDKEY'),
          "sasl.password": os.getenv('CCLOUDSECRET')
          }

sources = [FileSource(file_path=dct[key].get('path'),
                      schema_path=f'ingestd/avro/schemas/raw/{key}.avsc',
                      key_fields=dct[key].get('key_fields'),
                      value_fields=dct[key].get('value_fields'),
                      parser=parseDelimited) for key in dct.keys()]

producers = [avro.AvroProducer(**CONFIG,
                               default_key_schema=key_schema,
                               default_value_schema=value_schema,
                               schema_registry=os.getenv('SCHEMAREGISTRYURL'))
             for key_schema, value_schema in zip(key_schemas, value_schemas)]

for src, producer, topic in zip(sources, producers, dct.keys()):
    for specific_record in src.produce_payload(specific_flag=True):
        producer.produce(topic=topic,
                         key=dict({key_name: key_value for key_name, key_value
                                   in zip(specific_record.get('record_key_fields'),
                                          specific_record.get('record_key'))}),
                         value=dict({field_name: field_value for field_name, field_value
                                     in zip(specific_record.get('value_key_fields'),
                                            specific_record.get('record_value'))
                                     }),
                         callback=acked
                         )


def acked(err, msg):
    """
    Delivery report callback called (from flush()) on successful/failed delivery of msg.
    :param err:
    :param msg:
    :return:
    """
    if err is not None:
        print(f"Failed to delivery message {err.str()}")
    else:
        print(f"Produced to: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")