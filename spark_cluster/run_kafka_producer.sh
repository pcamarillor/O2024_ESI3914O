#!/bin/bash

docker exec -it kafka_producer python3 /opt/spark-apps/kafka_producers/kafka_sensor_producer.py --kafka-bootstrap a11ae9e81b04 --kafka-topic pyspark-example

