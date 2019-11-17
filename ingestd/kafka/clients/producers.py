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
