import queue
import threading
from time import sleep
import numpy as np
import time
import pymongo
from pymongo import MongoClient

# Coda condivisa per immagazzinare i dati
data_queue = queue.Queue()

# Connessione a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['stock_data']
collection = db['quotes']

# Thread worker
def worker(i):
    company = str(companies[i])
    quote = start_quote[i]
    while True:
        timestamp = round(time.time(), 3)
        data_queue.put((company, timestamp, quote))
        # nuovo prezzo
        quote = round(np.random.normal(quote, 0.1), 2)
        # attesa
        delay = np.random.normal(0.01, 0.1)
        sleep(abs(delay))

# Consumer
def consumer():
    while True:
        try:
            company, timestamp, quote = data_queue.get(timeout=1)
            print(f"Consumer ha ricevuto: {company} {timestamp} {quote}")
            # Inserisci il dato in MongoDB
            data = {"company": company, "timestamp": timestamp, "quote": quote}
            collection.insert_one(data)
            print(f"Dato memorizzato in MongoDB: {data}")
        except queue.Empty:
            continue

# Lista delle compagnie e prezzi iniziali
companies = ["Apple", "Microsoft", "Samsung", "Philips", "Toyota", "BMW", "FCA", "Amazon", "IBM", "Dell"]
start_quote = [1032.03, 131.23, 435.1, 641.23, 121.53, 344.9, 313.6, 931.34, 1112.97, 394.1]

# Avvia i worker
threads = []
for i in range(10):
    t = threading.Thread(target=worker, args=(i,))
    threads.append(t)
    t.start()

# Avvia il consumer
consumer_thread = threading.Thread(target=consumer)
consumer_thread.start()
