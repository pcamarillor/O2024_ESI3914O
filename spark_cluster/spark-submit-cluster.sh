#!/bin/bash

#docker run -d --name spark_submit_container --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /bin/bash -c "sleep infinity"

docker exec -it spark_submit_container /spark/bin/spark-submit \
--master spark://$1:7077 \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2 \
/opt/spark-apps/$2 --kafka-bootstrap $3


docker exec -it spark_submit_container /spark/bin/spark-submit --master spark://ea1f6426d52c:7077 --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2 /opt/spark-apps/stream_window_aggregations.py --kafka-bootstrap a11ae9e81b04


docker run -d --name kafka_producer --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 kafka-producer-app /bin/bash -c "sleep infinity"


# Run producer
docker exec -it kafka_producer python3 /opt/spark-apps/kafka_producers/kafka_sensor_producer.py --kafka-bootstrap a11ae9e81b04 --kafka-topic pyspark-example