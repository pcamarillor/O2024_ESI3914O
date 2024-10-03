#!/bin/bash
docker run --network spark_cluster_default \
--volumes-from spark_cluster-spark-master-1 \
-p 4041:4040 \
spark-submit \
/spark/bin/spark-submit \
--master spark://$1:7077 \
--deploy-mode client \
--packages org.mariadb.jdbc:mariadb-java-client:3.1.2 \
/opt/spark-apps/$2