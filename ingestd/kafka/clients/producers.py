import confluent_kafka as kafka
from confs import configuration


class ProducerFactory:
    def __init_(self):
        self._producers = {}

    def register_producer(self, key, producer: object):
        self._producers[key] = producer

    def create(self, key, **kwargs):
        producer = self._producers.get(key)
        if not producer:
            raise ValueError(key)
        return producer(**kwargs)


