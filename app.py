import os
import time
from kafka import KafkaConsumer
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.util import kafka_bytestring
from kafka.common import LeaderNotAvailableError, ConnectionError, KafkaUnavailableError

if __name__ == "__main__":
    # Fetch setting from env
    env_kafka_addr = os.environ.get('KAFKA_PORT_9092_TCP_ADDR')
    env_kafka_port = os.environ.get('KAFKA_PORT_9092_TCP_PORT')
    if env_kafka_addr is None:
        raise TypeError("Can't get Kafka address from environment variable")
    if env_kafka_port is None:
        raise TypeError("Can't get Kafka port from environment variable")
    try:
        env_kafka_port = int(env_kafka_port)
    except:
        raise TypeError("Couldn't turn Kafka port into a number")

    env_kafka_topic = os.environ.get('TOPIC')
    if env_kafka_topic is None:
        raise TypeError("Can't get Kafka topic from environment variable")

    env_consumer_group = os.environ.get('CONSUMER_GROUP')
    if env_consumer_group is None:
        raise TypeError("Can't get Kafka consumer group from environment variable")

    # Kafka settings
    consumer_group = kafka_bytestring(env_consumer_group)
    topic = kafka_bytestring(env_kafka_topic)
    
    kafka_host = "{0}:{1}".format(env_kafka_addr, env_kafka_port)

    # Create client
    print("Starting Kafka client: ", kafka_host, topic, consumer_group, flush=True)
    try:
        client = KafkaClient(kafka_host)
    except (LeaderNotAvailableError, ConnectionError, KafkaUnavailableError,) as leader_err:
        num_tries = 0
        max_tries = 10
        is_connected = False
        while not is_connected:
            print("Retrying to start consumer client: {0!s}".format(num_tries), flush=True)
            time.sleep(5)
            try:
                client = KafkaClient(kafka_host)
                is_connected = True
            except (LeaderNotAvailableError, ConnectionError, KafkaUnavailableError,) as leader_err2:
                num_tries += 1
                if num_tries == max_tries:
                    raise leader_err2
    # except Exception as err:
    #     print("Error starting consumer client: " + str(err), flush=True)
    #     raise err
    client.ensure_topic_exists(topic)

    # Create consumer
    try:
        consumer = SimpleConsumer(client, consumer_group, topic)
    except LeaderNotAvailableError as leader_err:
        num_tries = 0
        max_tries = 10
        is_connected = False
        while not is_connected:
            print("Retrying to start consumer: {0!s}".format(num_tries), flush=True)
            time.sleep(5)
            try:
                consumer = SimpleConsumer(client, consumer_group, topic)
                is_connected = True
            except LeaderNotAvailableError as leader_err2:
                num_tries += 1
                if num_tries == max_tries:
                    raise leader_err2
    except Exception as err:
        print("Error starting consumer: " + str(err), flush=True)
        raise err

    # Get messages
    print("Going into message loop", flush=True)
    for message in consumer:
        # message value is raw byte string -- decode if necessary
        # e.g., for unicode: message.value.decode('utf-8')
        print(message, flush=True)
        # print("{0!s}:{1!s}:{2!s}: key={3!s} value={4!s}".format(message.topic, message.partition,
        #                                      message.offset, message.key,
        #                                      message.value), flush=True)