#!/bin/bash

docker run --network spark_cluster_default \
--volumes -from spark_cluster-spark-master-1 \
-p 4041:4040 \
spark-submit \
/spark/bin/spark-submit \
--master spark://ad84c2daf91d:7077 \
--deploy-mode client \
/opt/spark-apps/solution_lab07_03.py

#ad84c2daf91d
#docker run --network spark_cluster_default --volumes-from spark_cluster-spark-master-1 -p 4041:4040 spark-submit /spark/bin/spark-submit --master spark://ad84c2daf91d:7077 --deploy-mode client /opt/spark-apps/solution_lab07_03.py
