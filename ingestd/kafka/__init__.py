import ingestd.kafka.consumers
import ingestd.kafka.operators
import ingestd.kafka.producer


finwire_field_widths = {"CMP": (15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150),
                        "SEC": (15, 3, 15, 6, 4, 706, 13, 8, 8, 12, 60),
                        "FIN": (15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60)}

