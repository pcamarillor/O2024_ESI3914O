#!/bin/bash

docker run --network spark_cluster_default \
--volumes-from spark_cluster-spark-master-1 \
-p 4041:4040 \
spark-submit \
/spark/bin/spark-submit \
--master spark://$1:7077 \
--deploy-mode client \
/opt/spark-apps/$2
