import fastavro as avro


def consume(self):
    for msg in self.kafka_consumer:
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        yield reader.read
