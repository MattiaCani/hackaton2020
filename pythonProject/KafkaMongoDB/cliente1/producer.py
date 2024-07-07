from kafka import KafkaProducer
import numpy as np
import threading
from time import sleep
import time
import json

# Configura il producer di Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lista delle compagnie e prezzi iniziali
companies = ["Apple", "Microsoft", "Samsung", "Philips", "Toyota", "BMW", "FCA", "Amazon", "IBM", "Dell"]
start_quote = [1032.03, 131.23, 435.1, 641.23, 121.53, 344.9, 313.6, 931.34, 1112.97, 394.1]

# Funzione worker per generare dati e inviarli a Kafka
def worker(i):
    company = str(companies[i])
    quote = start_quote[i]
    while True:
        timestamp = round(time.time(), 3)
        data = {"company": company, "timestamp": timestamp, "quote": quote}
        producer.send('stock_quotes', value=data)
        print(f"Messaggio inviato: {data}")
        # nuovo prezzo
        quote = round(np.random.normal(quote, 0.1), 2)
        # attesa
        delay = np.random.normal(0.01, 0.1)
        sleep(abs(delay))

def start_producer():
    threads = []
    for i in range(10):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    start_producer()
