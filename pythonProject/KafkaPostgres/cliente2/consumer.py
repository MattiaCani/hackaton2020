from kafka import KafkaConsumer
import json
import psycopg2

# Configurazione di PostgreSQL
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_USER = 'username'
DB_PASSWORD = 'password'
DB_NAME = 'client_database'

# Connessione a PostgreSQL
conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
cursor = conn.cursor()

# Configura il consumer di Kafka
consumer = KafkaConsumer(
    'selected_stock_quotes',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='client_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


# Funzione per consumare e memorizzare le quotazioni su PostgreSQL
def consume_and_store():
    for message in consumer:
        data = message.value
        company = data['company']
        timestamp = data['timestamp']
        quote = data['quote']

        # Esegui l'inserimento nel database PostgreSQL
        insert_query = "INSERT INTO stock_quotes (company, timestamp, quote) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (company, timestamp, quote))
        conn.commit()
        print(f"Dato memorizzato su PostgreSQL: {data}")


def start_consumer():
    consume_and_store()


if __name__ == "__main__":
    start_consumer()
