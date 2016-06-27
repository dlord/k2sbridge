from kafka import KafkaProducer
import stomp, logging, os, threading

CLIENT_ID = os.getenv('KAFKA_CLIENT_ID', 'k2sbridge-producer')
MAX_RETRY = int(os.getenv('KAFKA_MAX_RETRY', '3'))

def start():
    logging.info('Starting producer')
    server_address = '%s:%s' % (os.getenv('KAFKA_HOST', 'localhost'), os.getenv('KAFKA_PORT', '9092'))

    kafka_opts_defaults = {
        'client_id'                              : CLIENT_ID,
        'retries'                                : MAX_RETRY,
        'batch_size'                             : 16384,
        'linger_ms'                              : 0,
        'max_block_ms'                           : 60000,
        'max_request_size'                       : 1048576,
        'metadata_max_age_ms'                    : 300000,
        'retry_backoff_ms'                       : 100,
        'request_timeout_ms'                     : 30000,
        'reconnect_backoff_ms'                   : 50,
        'max_in_flight_requests_per_connection'  : 5,
        'security_protocol'                      : 'PLAINTEXT',
        'acks'                                   : 'all',
        'bootstrap_servers'                      : server_address,
    }

    producer = KafkaProducer(**kafka_opts_defaults)

    class K2SBridgeListener(stomp.ConnectionListener):
        def on_error(self, headers, message):
            logging.error('received an error "%s"' % message)

        def on_message(self, headers, message):
            logging.info('received a message "%s"' % message)
            producer.send(os.getenv('KAFKA_TOPIC'), value=message)


    stomp_conn = stomp.Connection([(os.getenv('STOMP_HOST', 'localhost'), os.getenv('STOMP_PORT', '61613'))],
                                  auto_decode=False)
    stomp_conn.set_listener('', K2SBridgeListener())
    stomp_conn.start()
    stomp_conn.connect(wait=True)

    stomp_conn.subscribe(destination=os.getenv('STOMP_TOPIC'), id=1, ack='auto')

    condition = threading.Condition()

    condition.acquire()
    while True:
        condition.wait()

    stomp_conn.disconnect()
