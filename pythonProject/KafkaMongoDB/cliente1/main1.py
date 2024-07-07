from multiprocessing import Process
import producer
import consumer
from flask import Flask

# Importa le funzioni dell'API Flask
from app1 import app as flask_app

def main():
    # Avvia il producer in un processo separato
    producer_process = Process(target=producer.start_producer)
    producer_process.start()

    # Avvia il consumer in un processo separato
    consumer_process = Process(target=consumer.start_consumer)
    consumer_process.start()

    # Avvia il server Flask nell'ambiente principale
    flask_process = Process(target=flask_app.run, kwargs={'debug': True})
    flask_process.start()

    # Attendi la terminazione di tutti i processi
    producer_process.join()
    consumer_process.join()
    flask_process.join()

if __name__ == "__main__":
    main()
