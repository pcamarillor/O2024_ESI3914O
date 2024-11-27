#!/bin/bash

docker exec -it kafka_producer python3 /opt/spark-apps/ProyectoTeam4/kafka_producer.py --kafka-bootstrap $1 --kafka-topic $2

