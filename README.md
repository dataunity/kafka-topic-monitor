# Kafka Topic Monitor
Simple Kafka Consumer to monitor a Topic. Compatible with Docker Compose.

## Usage
To use the Kafka Topic Monitor in a Docker Compose project add something like the following to your Compose config file. The config file should have images for Kafka and Zookeeper. Change the CONSUMER_GROUP environment to something unique and set the TOPIC environment variable to the topic you want to monitor. Optionally change the links to kafka and zookeeper to match the names used in your Docker Compose config file.

    tmpmonitor:
        build: ../kafka-topic-monitor
        links:
            - kafka
            - zookeeper
        environment:
            CONSUMER_GROUP: MyMonitor01
            TOPIC: MyTopic
