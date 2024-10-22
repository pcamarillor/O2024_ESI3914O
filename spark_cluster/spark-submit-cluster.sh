#!/bin/bash

#docker run -d --name spark_submit_container --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /bin/bash -c "sleep infinity"

docker exec -it spark_submit_container /spark/bin/spark-submit \
--master spark://$1:7077 \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2 \
/opt/spark-apps/$2 --kafka-bootstrap $3

