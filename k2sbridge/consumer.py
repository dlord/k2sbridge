from kafka import KafkaConsumer
import stomp, logging, os

def start():
    logging.info("Starting consumer")
    assert os.getenv('KAFKA_TOPIC')
    consumer = KafkaConsumer(os.getenv('KAFKA_TOPIC'),
                             bootstrap_servers=['%s:%s' % (os.getenv('KAFKA_HOST', 'localhost'), os.getenv('KAFKA_PORT', '9092'))])

    stomp_conn = stomp.Connection([(os.getenv('STOMP_HOST', 'localhost'), os.getenv('STOMP_PORT', '61613'))],
                                  auto_decode=False))
    stomp_conn.set_listener('', stomp.PrintingListener())
    stomp_conn.start()
    stomp_conn.connect(wait=True)

    logging.info("Waiting for data...")
    for data in consumer:
        print(data.value)
        stomp_conn.send(body=data.value, destination=os.getenv('STOMP_TOPIC'))

    logging.info("Shutting down...")
