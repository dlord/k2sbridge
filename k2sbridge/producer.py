from kafka import KafkaClient, SimpleProducer
import stomp, logging, os, threading


logging.info('Starting producer')
kafka = KafkaClient('%s:%s' % (os.getenv('KAFKA_HOST', 'localhost'), os.getenv('KAFKA_PORT', '9092')))
producer = SimpleProducer(kafka)

class K2SBridgeListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        logging.error('received an error "%s"' % message)

    def on_message(self, headers, message):
        logging.info('received a message "%s"' % message)
        producer.send_messages(os.getenv('KAFKA_TOPIC'), message)


logging.info("Starting consumer")
stomp_conn = stomp.Connection([(os.getenv('STOMP_HOST', 'localhost'), os.getenv('STOMP_PORT', '61613'))])
stomp_conn.set_listener('', K2SBridgeListener())
stomp_conn.start()
stomp_conn.connect(wait=True)

stomp_conn.subscribe(destination=os.getenv('STOMP_TOPIC'), id=1, ack='auto')

condition = threading.Condition()

condition.acquire()
while True:
    condition.wait()

stomp_conn.disconnect()
