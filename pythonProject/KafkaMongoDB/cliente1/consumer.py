from kafka import KafkaConsumer
import pymongo
import json
from datetime import datetime, timedelta
import threading
import statistics

# Connessione a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['stock_data']
collection = db['quotes']

# Configura il consumer di Kafka
consumer = KafkaConsumer(
    'stock_quotes',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mygroup',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Funzione per calcolare variazione percentuale
def percent_change(old_value, new_value):
    return ((new_value - old_value) / old_value) * 100

# Funzione per ottenere il timestamp 2 minuti fa
def get_two_minutes_ago_timestamp():
    return datetime.now() - timedelta(minutes=2)

# Funzione per notificare le azioni che hanno guadagnato più dello 0.2% negli ultimi 2 minuti
def notify_gainers_and_losers(data):
    two_minutes_ago = get_two_minutes_ago_timestamp()
    if data['timestamp'] > two_minutes_ago:
        old_value = collection.find_one({'company': data['company']}, sort=[('timestamp', pymongo.DESCENDING)])['quote']
        change_percent = percent_change(old_value, data['quote'])
        if change_percent > 0.2:
            print(f"Azione guadagnata: {data['company']} - Variazione: {change_percent}%")
        elif change_percent < -1:
            print(f"Azione persa: {data['company']} - Variazione: {change_percent}%")

# Funzione per notificare media e deviazione standard dei valori di un titolo negli ultimi 5, 10, e 30 minuti
def notify_average_and_std(data):
    for interval in [5, 10, 30]:
        time_ago = datetime.now() - timedelta(minutes=interval)
        values = [d['quote'] for d in collection.find({'company': data['company'], 'timestamp': {'$gt': time_ago}})]
        if values:
            avg_value = sum(values) / len(values)
            std_deviation = statistics.stdev(values) if len(values) > 1 else None
            print(f"Media dei valori per {interval} minuti: {avg_value}")
            print(f"Deviazione standard per {interval} minuti: {std_deviation}")

# Funzione per notificare quanti valori sono stati ricevuti negli ultimi 10 secondi
def notify_count_last_10_seconds(data):
    ten_seconds_ago = datetime.now() - timedelta(seconds=10)
    count = collection.count_documents({'company': data['company'], 'timestamp': {'$gt': ten_seconds_ago}})
    print(f"Numero di valori ricevuti negli ultimi 10 secondi per {data['company']}: {count}")

# Funzione per gestire i cambiamenti di MongoDB Change Stream
def watch_collection_changes():
    pipeline = [
        {'$match': {'operationType': {'$in': ['insert', 'update', 'replace']}}},
        {'$addFields': {
            'change_percent': {
                '$cond': {
                    'if': {
                        '$gte': ['$quote', {'$multiply': ['$old_quote', 1.002]}]  # Esempio: aumentato del 0.2%
                    },
                    'then': {'$subtract': ['$quote', '$old_quote']},
                    'else': None  # Non aggiungere il campo se non c'è cambiamento significativo
                }
            }
        }}
    ]

    with collection.watch(pipeline=pipeline) as stream:
        for change in stream:
            data = change['fullDocument']
            notify_gainers_and_losers(data)
            notify_average_and_std(data)
            notify_count_last_10_seconds(data)
            print(f"Change Stream Event: {data}")

# Avvia il consumatore Kafka e monitora i cambiamenti nella collezione MongoDB
def start_consumer():
    # Avvia un thread per monitorare i cambiamenti nella collezione MongoDB
    change_thread = threading.Thread(target=watch_collection_changes)
    change_thread.start()

    # Avvia il consumer di Kafka
    for message in consumer:
        data = message.value
        data['old_quote'] = collection.find_one({'company': data['company']}, sort=[('timestamp', pymongo.DESCENDING)])['quote'] if collection.count_documents({'company': data['company']}) > 0 else data['quote']

        # Memorizza il dato in MongoDB
        collection.insert_one(data)
        print(f"Dato memorizzato in MongoDB: {data}")

if __name__ == "__main__":
    start_consumer()
