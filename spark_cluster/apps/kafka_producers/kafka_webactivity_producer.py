import argparse
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Función para generar datos de usuario simulados
def generate_user_data():
    return {
        "user_id": random.randint(1, 1000),
        "page": random.choice(["home", "product", "checkout"]),
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "clicks": random.randint(1, 10)
    }

if __name__ == "__main__":
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Kafka Producer")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap server")
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic")
    args = parser.parse_args()

    # Configurar el productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialización de JSON
        )
        print(f"Conectado al servidor Kafka: {args.kafka_bootstrap}")

    except Exception as e:
        print(f"Error al conectar con el servidor Kafka: {e}")
        exit(1)

    # Enviar datos al tópico
    try:
        print(f"Produciendo mensajes al tópico: {args.kafka_topic}")
        while True:
            message = generate_user_data()
            producer.send(args.kafka_topic, value=message)
            print(f"Mensaje enviado: {message}")
            
            # Registrar en un archivo de logs
            with open(f"{args.kafka_topic}_producer.log", "a") as log_file:
                log_file.write(f"{message}\n")

            time.sleep(1)  # Enviar un mensaje cada segundo

    except KeyboardInterrupt:
        print("\nProducción de mensajes detenida por el usuario.")

    except Exception as e:
        print(f"Error al enviar mensaje: {e}")

    finally:
        producer.close()
        print("Productor Kafka cerrado.")
