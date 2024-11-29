import subprocess
import os

# Paths absolutos de los scripts
CLICK_PRODUCER_PATH = '/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/producers/clickProducer.py'
VIEWS_PRODUCER_PATH = '/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/producers/viewsProducer.py'
CONSUMER_PATH = '/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/consumer.py'
BATCH_PROCESSING_PATH = '/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/batch_processing.py'
POSTGRES_JAR_PATH = '/Users/franciscofloresenriquez/O2024_ESI3914O/finalProject_team09/postgresql-42.7.4.jar'
SPARK_CLUSTER_DIR = '/Users/franciscofloresenriquez/O2024_ESI3914O/spark_cluster'
KAFKA_CLUSTER_DIR = '/Users/franciscofloresenriquez/O2024_ESI3914O/kafka_cluster'

# Procesos activos
processes = []


def run_click_producer():
    print("Running clickProducer.py...")
    process = subprocess.Popen(['python', CLICK_PRODUCER_PATH])
    processes.append(process)


def run_views_producer():
    print("Running viewsProducer.py...")
    process = subprocess.Popen(['python', VIEWS_PRODUCER_PATH])
    processes.append(process)


def run_consumer():
    print("Running consumer.py...")
    process = subprocess.Popen([
        'spark-submit',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2',
        CONSUMER_PATH
    ])
    processes.append(process)


def run_batch_processing():
    print("Running batch_processing.py...")
    process = subprocess.Popen([
        'spark-submit',
        '--jars', POSTGRES_JAR_PATH,
        BATCH_PROCESSING_PATH
    ])
    processes.append(process)


def start_postgres_container():
    print("Starting PostgreSQL container...")
    subprocess.run(['docker', 'start', 'postgres_auth'])


def start_spark_cluster():
    print("Starting Spark cluster...")
    subprocess.run(['docker', 'compose', 'up', '--scale', 'spark-worker=3'], cwd=SPARK_CLUSTER_DIR)


def start_kafka_cluster():
    print("Starting Kafka cluster...")
    subprocess.run(['docker', 'compose', 'up', '-d'], cwd=KAFKA_CLUSTER_DIR)


def stop_all_processes():
    print("Stopping all running processes...")
    for process in processes:
        process.terminate()
    processes.clear()


def main_menu():
    while True:
        print("\nMenu")
        print("1. Run clickProducer.py")
        print("2. Run viewsProducer.py")
        print("3. Run both producers")
        print("4. Run consumer.py")
        print("5. Run batch_processing.py")
        print("6. Start PostgreSQL container")
        print("7. Start Spark cluster")
        print("8. Start Kafka cluster")
        print("9. Stop all running processes")
        print("10. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            run_click_producer()
        elif choice == "2":
            run_views_producer()
        elif choice == "3":
            run_click_producer()
            run_views_producer()
        elif choice == "4":
            run_consumer()
        elif choice == "5":
            run_batch_processing()
        elif choice == "6":
            start_postgres_container()
        elif choice == "7":
            start_spark_cluster()
        elif choice == "8":
            start_kafka_cluster()
        elif choice == "9":
            stop_all_processes()
        elif choice == "10":
            print("Exiting the menu.")
            stop_all_processes()
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main_menu()