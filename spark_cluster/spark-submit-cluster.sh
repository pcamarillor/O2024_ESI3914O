#!/bin/bash

docker run --rm --network spark_cluster_default \
--volumes-from spark_cluster-spark-master-1 \
-p 4041:4040 \
spark-submit \
/spark/bin/spark-submit \
--master spark://$1:7077 \
--deploy-mode client --packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.0 \
/opt/spark-apps/$2
