# Spark Cluster with Docker & docker-compose

# General

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose installed

* A PySpark Application Jar to play with

## Build the docker images

The first step to deploy the cluster will be the build of the custom images, these builds can be performed with the *build-images.sh* script. 

The executions is as simple as the following steps:

For Linux/MacOS users:

```sh
chmod +x build-images.sh
./build-images.sh
```

For Windows users:

```sh
./build-images.bat
```

This will create the following docker images:

* spark-base:3.5.2: A base image based on java:alpine-jdk-8 wich ships scala, python3 and spark 3.5.2.

* spark-master:3.5.2: A image based on the previously created spark image, used to create a spark master containers.

* spark-worker:3.5.2: A image based on the previously created spark image, used to create spark worker containers.

* spark-submit:3.5.2: A image based on the previously created spark image, used to create spark submit containers(run, deliver driver and die gracefully).

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker compose up --scale spark-worker=3
```

## Validate your cluster

Just validate your cluster accessing the spark UI on [master URL](http://localhost:9090)


### Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
/mnt/spark-apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
/mnt/spark-data|/opt/spark-data| Used to make available your app's data on all workers & master


# Run a sample application

Now let`s make a **wild spark submit** to validate the distributed nature of our new toy following these steps:

## Create a PySpark app

The first thing you need to do is to make a spark application. 

In my case I am using an app called hello-world app located in `spark_cluster/apps/` directory.

## Ship your jar & dependencies on the Workers and Master

A necesary step to make a **spark-submit** is to copy your application bundle into all workers, also any configuration file or input file you need.

Luckily for us we are using docker volumes so, you just have to copy your app and configs into `spark_cluster/apps` directory

## Check the successful copy of the data and app jar (Optional)

This is not a necessary step, just if you are curious you can check if your app code and files are in place before running the spark-submit.

```sh
# Worker 1 Validations
docker exec -ti spark-worker-1 ls -l /opt/spark-apps

docker exec -ti spark-worker-1 ls -l /opt/spark-data

# Worker 2 Validations
docker exec -ti spark-worker-2 ls -l /opt/spark-apps

docker exec -ti spark-worker-2 ls -l /opt/spark-data

# Worker 3 Validations
docker exec -ti spark-worker-3 ls -l /opt/spark-apps

docker exec -ti spark-worker-3 ls -l /opt/spark-data
```
After running one of this commands you have to see your app's python files.


## Use docker spark-submit

```bash
#We have to use the same network as the spark cluster(internally the image resolves spark master as spark://<container_id>:7077)
docker run --network spark_cluster_default \
--volumes-from spark_cluster-spark-master-1 \
spark-submit \
/spark/bin/spark-submit \
--master spark://<container_id>:7077 \
--deploy-mode client \
/opt/spark-apps/solution.py
```

After running this you will see an output pretty much like this:

```bash
24/09/03 00:29:50 INFO SparkContext: Running Spark version 3.5.2
24/09/03 00:29:50 INFO SparkContext: OS info Linux, 6.6.12-linuxkit, aarch64
24/09/03 00:29:50 INFO SparkContext: Java version 17.0.12
...
24/09/03 00:29:50 INFO SparkContext: Submitted application: ITESO-BigData-Hello-World-App
24/09/03 00:29:50 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
...
Spark Master URL: spark://005393f60740:7077
Hello World App - ITESO - Big Data - 2024
apple: 3
banana: 2
orange: 3s
```



