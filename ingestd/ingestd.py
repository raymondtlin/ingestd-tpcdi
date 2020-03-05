#!/usr/bin/env python3

import sys
import click
from glob import glob


class ProducerRC(object):
    def __init__(self, conf, glob=None):
        self.conf = conf
        self.glob = glob


@click.group()
def cli():
    pass


@cli.command()
@click.argument('--glob', type=click.STRING, required=True)
def producer(config, glob):
    # ctx.obj = ProducerRC(configure(config), glob)
    rt_confs, rc_key_schemas, rc_value_schemas = build_rc(glob)
    rc_sources = create_sources(rt_confs)

    producers = [
        confluent_kafka.avro.AvroProducer(
            **CONFIG,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
            schema_registry=os.getenv('SCHEMA_REGISTRY_URL')
            ) for key_schema, value_schema in zip(
                rc_key_schemas, rc_value_schemas)
        ]

    for src, producer, topic in zip(rc_sources, producers, rt_confs.keys()):
        for specific_record in src.produce_payload(specific_flag=True):
            producer.produce(
                topic=topic,
                key=dict({key_name: key_value for key_name, key_value in
                          zip(specific_record.get('record_key_fields'),
                              specific_record.get('record_key'))}),
                value=dict(
                    {field_name: field_value for field_name, field_value in
                        zip(specific_record.get('value_key_fields'),
                            specific_record.get('record_value'))}),
                callback=acked)


def configure(config):
    if config:
        CONFIG = config
    else:
        CONFIG = {"bootstrap.servers": os.getenv('BOOTSTRAP_SERVERS'),
                  "sasl.mechanisms": "PLAIN",
                  "security.protocol": "SASL_SSL",
                  "sasl.username": os.getenv('CCLOUD_KEY'),
                  "sasl.password": os.getenv('CCLOUD_SECRET')}
    return CONFIG


def build_rc(glob):
    """
    Creates runtime configuration for parsed producer args.
    :return:
    """
    dct = {}
    key_schemas, value_schemas = [], []
    schema_root = "ingestd/avro/schemas/"

    files = [file for file in glob(f"/data/{glob}")]

    for file in files:
        dct.update(
            dict({file.split('/')[-1].split('.')[0]: file}))

    for key in dct.keys():
        with open(schema_root + f"key/{key}Key.avsc") as avroKeySchema:
            key_schemas.append(
                confluent_kafka.avro.loads(avroKeySchema.read()))

        with open(schema_root + f"value/{key}Value.avsc") as avroValueSchema:
            value_schemas.append(
                confluent_kafka.avro.loads(avroValueSchema.read()))

    for (k, v), key_fields, value_fields in zip(dct.items(),
                                                key_schemas,
                                                value_schemas):
        dct.update(
            dict(
                {k: {
                    "path": v,
                    "key_fields": [
                        field.get('name') for field in key_fields.to_json().get('fields')],
                    "value_fields": [
                        field.get('name') for field in value_fields.to_json().get('fields')]
                    }
                }
            )
        )

    return dct, key_schemas, value_schemas


def create_sources(rc: dict) -> list:
    sources = [
        ingestd.sources.FileSource(
            file_path=rc[key].get('path'),
            schema_path=schema_root + f"raw/{key}.avsc",
            key_fields=rc[key].get('key_fields'),
            value_fields=rc[key].get('value_fields'),
            parser=determine_parser(args.glob.split('.')[-1]))
        for key in rc.keys()]
    return sources


def determine_parser(fileformat: str):
    record_parse_map = {"fwf": ingestd.strategies.parseFixedWidth,
                        "csv": ingestd.strategies.parseDelimited,
                        "txt": ingestd.strategies.parseDelimited,
                        "xml": ingestd.strategies.parseXML}

    return record_parse_map.get(fileformat)


def acked(err, msg):
    """
    Delivery report callback called
        (from flush()) on successful/failed delivery of msg.
    :param err:
    :param msg:
    :return:
    """
    if err is not None:
        print(f"Failed to delivery message {err.str()}")
    else:
        print(f"Produced to: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


if __name__ == '__main__':
    cli()