from __future__ import absolute_import

import ingestd.factories
from ingestd.sources import *

import ingestd.kafka.clients.producers as producers
import ingestd.kafka.clients.consumers as consumers
import ingestd.kafka.clients.operators as operators

from glob import glob
from confluent_kafka.avro import AvroProducer, AvroConsumer

#
# def main():
#     finwire_field_widths = {"CMP": (15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150),
#                             "SEC": (15, 3, 15, 6, 4, 706, 13, 8, 8, 12, 60),
#                             "FIN": (15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60)}
#
#     strat_factory = ingestd.factories.StrategyFactory()
#     source_factory = ingestd.factories.SourceFactory()
#
#     strat_factory.register_strategy('xml', RecordParsingStrategy('xmlparse', parseXML(iterable=None)))
#
#
#
#
#     dispatch_tbl = {"xml": operators.XMLOperator,
#                     "csv": operators.DelimitedOperator,
#                     "txt": operators.DelimitedOperator,
#                     "finwire": operators.FixedWidthOperator}
#
#     files = glob('data/*')
#
#     map(lambda x: o.register_operator(), files)
#
#
#         if file in glob(f'data/{format_key}'):
#             o.register_operator(file.split('/')[1],
#                               operators.DelimitedOperator(file, '|')
#                               )
#
#     o.register_format('XML', operators.XMLOperator('data/CustomerMgmt.xml'))
#     o.register_format('CSV',
#                       operators.DelimitedOperator('data/HR.csv',
#                                                   ','))
#     o.register_format('FINWIRE',
#                       operators.FixedWidthOperator('data/finwire',
#                                                    field_widths_lookup=finwire_field_widths))
#
#     brokers = ['127.0.0.1:9092']
#
#     config = {'bootstrap.servers': brokers}
#
#     factory = producers.ProducerFactory()
#
#     factory.register_producer('finwire',
#                               AvroProducer(config=config,
#                                            schema_registry='https://localhost:8081'
#                                            )
#                               )
#
#
#     fw = o.get_operator('FINWIRE')
#
#     for record in fw.create_handle():
#         rec_type = fw.classify_record(record)
#         print(rec_type)
#         rfw = fw.field_width_lookup[rec_type]
#         print(rec_type, fw.create_parser(rfw)(record))
#
#
# main()


