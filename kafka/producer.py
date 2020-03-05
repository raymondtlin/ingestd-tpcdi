<<<<<<< HEAD:kafka/producer.py
#!/usr/bin/env python3


import os
=======
>>>>>>> 12de8f4e6a8128dea3e9852db3b38a13bd49f4ff:ingestd/kafka/producer.py
import argparse
import os
from glob import glob

import confluent_kafka
<<<<<<< HEAD:kafka/producer.py
from ingestd-tpcdi import ingestd.sources
from ingestd-tpcdi import ingestd.strategies
from glob import glob
=======

import ingestd
>>>>>>> 12de8f4e6a8128dea3e9852db3b38a13bd49f4ff:ingestd/kafka/producer.py

parser = argparse.ArgumentParser(description="Producer")

parser.add_argument('--config',
                    type=list,
                    required=False,
                    help='Kafka Config')

parser.add_argument('--glob',
                    type=str,
                    required=True,
                    help='Glob pattern indicating file(s) to include')

parser.add_argument('--single',
                    default=False,
                    type=bool,
                    required=True,
                    help='Single or multiple files')

args = parser.parse_args()


def build_rc():
    """
    Creates runtime configuration for parsed producer args.
    :return:
    """

    dct = {}
    key_schemas, value_schemas = [], []
    files = [file for file in glob(f'/data/{args.glob}')]

    for file in files:
        dct.update(
            dict(
                {file.split('/')[-1].split('.')[0]: file}
            )
        )

    for key in dct.keys():
        with open(f'ingestd/avro/schemas/key/{key}Key.avsc') as avroKeySchema:
            key_schemas.append(confluent_kafka.avro.loads(avroKeySchema.read()))

        with open(f'ingestd/avro/schemas/value/{key}Value.avsc') as avroValueSchema:
            value_schemas.append(confluent_kafka.avro.loads(avroValueSchema.read()))

    for (k, v), key_fields, value_fields in zip(dct.items(), key_schemas, value_schemas):
        dct.update(
            dict(
                {k: {"path": v,
                     "key_fields": [field.get('name')
                                    for field in key_fields.to_json().get('fields')],
                     "value_fields": [field.get('name')
                                      for field in value_fields.to_json().get('fields')]
                     }}
            )
        )

    return dct, key_schemas, value_schemas


def create_sources(rc: dict) -> list:
    """
    Function to instantiate a list of FileSources
    """
    sources = [ingestd.sources.FileSource(file_path=rc[key].get('path'),
                                          schema_path=f'ingestd/avro/schemas/raw/{key}.avsc',
                                          key_fields=rc[key].get('key_fields'),
                                          value_fields=rc[key].get('value_fields'),
                                          parser=determine_parser(args.glob.split('.')[-1]))
               for key in rc.keys()]

    return sources


def determine_parser(fileformat: str):
    """
    Returns a parsers signature
    """
    record_parse_map = {"fwf": ingestd.strategies.parseFixedWidth,
                        "csv": ingestd.strategies.parseDelimited,
                        "txt": ingestd.strategies.parseDelimited,
                        "xml": ingestd.strategies.parseXML}

    return record_parse_map.get(fileformat)


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


if __name__ == "__main__":
    if args.config:
        CONFIG = args.config
    else:
        CONFIG = {"bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
                  "sasl.mechanisms": "PLAIN",
                  "security.protocol": "SASL_SSL",
                  "sasl.username": os.getenv('CCLOUD_KEY'),
                  "sasl.password": os.getenv('CCLOUD_SECRET')}

    RUNTIME_CONFS, RC_KEY_SCHEMAS, RC_VALUE_SCHEMAS = build_rc()
    rc_sources = create_sources(RUNTIME_CONFS)

    producers = [confluent_kafka.avro.AvroProducer(**CONFIG,
                                                   default_key_schema=key_schema,
                                                   default_value_schema=value_schema,
                                                   schema_registry=os.getenv('SCHEMA_REGISTRY_URL'))
                 for key_schema, value_schema in zip(RC_KEY_SCHEMAS, RC_VALUE_SCHEMAS)]

    for src, producer, topic in zip(rc_sources, producers, RUNTIME_CONFS.keys()):
        for specific_record in src.produce_payload(specific_flag=True):
            producer.produce(topic=topic,
                             key=dict({key_name: key_value for key_name, key_value
                                       in zip(specific_record.get('record_key_fields'),
                                              specific_record.get('record_key'))}),
                             value=dict({field_name: field_value for field_name, field_value
                                         in zip(specific_record.get('value_key_fields'),
                                                specific_record.get('record_value'))
                                         }),
                             callback=acked)
