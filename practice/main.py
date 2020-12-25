from kafka import KafkaProducer, KafkaConsumer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':

        kafka_producer = connect_kafka_producer()
        for i in range(11,16):
            publish_message(kafka_producer, 'test', 'values', str(i))
        if kafka_producer is not None:
            kafka_producer.close()
        consumer = KafkaConsumer('test',auto_offset_reset = 'earliest',bootstrap_servers = ['localhost:9092'],api_version = (0,10),consumer_timeout_ms=1000)
        for msg in consumer:
            print(msg.value)