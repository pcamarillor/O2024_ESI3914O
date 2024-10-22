#!/bin/bash

docker exec -it kafka_producer python3 /opt/spark-apps/$1 --kafka-bootstrap $2 --kafka-topic $3

