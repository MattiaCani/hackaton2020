from kafka import KafkaProducer
import json
import threading
from time import sleep
import numpy as np
import time

# Configura il producer di Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Funzione worker per generare dati e inviarli a Kafka
def worker(companies, selected_companies):
    while True:
        for company in selected_companies:
            quote = round(np.random.uniform(100, 1000), 2)  # Simulazione del valore di quotazione
            timestamp = round(time.time(), 3)
            data = {"company": company, "timestamp": timestamp, "quote": quote}
            producer.send('selected_stock_quotes', value=data)
            print(f"Messaggio inviato: {data}")
            sleep(1)  # Simulazione dell'intervallo tra i messaggi

def start_producer(selected_companies):
    companies = ["Apple", "Microsoft", "Samsung", "Philips", "Toyota", "BMW", "FCA", "Amazon", "IBM", "Dell"]
    threads = []
    for i in range(10):  # Simula 10 thread, uno per ogni azienda
        t = threading.Thread(target=worker, args=(companies, selected_companies))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    # Esempio di titoli selezionati dal cliente
    selected_companies = ["Apple", "Microsoft", "Amazon"]
    start_producer(selected_companies)
