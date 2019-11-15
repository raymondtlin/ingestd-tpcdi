import ingestd.kafka.clients.producers
import ingestd.kafka.clients.consumers
import ingestd.kafka.clients.operators
import confluent_kafka as kafka



def main():

    finwire_field_widths = {"CMP": (15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150),
                            "SEC": (15, 3, 15, 6, 4, 706, 13, 8, 8, 12, 60),
                            "FIN": (15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60)}

    o = operators.OperatorFactory()
    o.register_format('XML', operators.XMLOperator('/home/debian/Downloads/finwire'))
    o.register_format('CSV', operators.DelimitedOperator('/home/debian/Downloads/tpc-di_Batch1_HR.csv'))
    o.register_format('FINWIRE', operators.FixedWidthOperator('/home/debian/Downloads/finwire', field_widths=finwire_field_widths))


    k = kafka.Producer(config)

    for topic in ("finwirecmp", "finwiresec", "finwirefin"):
        conf = configuration.get('producer').get(topic)
        topic_name = topic
        factory.register_producer(topic, kafka.Producer(config=conf))