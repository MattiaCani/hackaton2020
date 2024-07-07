from flask import Flask, jsonify
import pymongo
from datetime import datetime, timedelta
import statistics

app = Flask(__name__)

# Connessione a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['stock_data']
collection = db['quotes']

# Funzione per ottenere il timestamp 2 minuti fa
def get_two_minutes_ago_timestamp():
    return datetime.now() - timedelta(minutes=2)

# Endpoint per ottenere azioni che hanno guadagnato più dello 0.2% negli ultimi 2 minuti
@app.route('/api/gainers', methods=['GET'])
def get_gainers():
    try:
        two_minutes_ago = get_two_minutes_ago_timestamp()
        gainers = []
        for data in collection.find({'timestamp': {'$gt': two_minutes_ago}}):
            old_value = collection.find_one({'company': data['company']}, sort=[('timestamp', pymongo.DESCENDING)])['quote']
            change_percent = ((data['quote'] - old_value) / old_value) * 100
            if change_percent > 0.2:
                gainers.append({'company': data['company'], 'change_percent': change_percent})
        return jsonify(gainers)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint per ottenere azioni che hanno perso più dell'1% negli ultimi 10 minuti
@app.route('/api/losers', methods=['GET'])
def get_losers():
    try:
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        losers = []
        for data in collection.find({'timestamp': {'$gt': ten_minutes_ago}}):
            old_value = collection.find_one({'company': data['company']}, sort=[('timestamp', pymongo.DESCENDING)])['quote']
            change_percent = ((data['quote'] - old_value) / old_value) * 100
            if change_percent < -1:
                losers.append({'company': data['company'], 'change_percent': change_percent})
        return jsonify(losers)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint per ottenere media e deviazione standard dei valori di un titolo negli ultimi 5, 10, e 30 minuti
@app.route('/api/statistics/<company>', methods=['GET'])
def get_statistics(company):
    try:
        stats = {}
        for interval in [5, 10, 30]:
            time_ago = datetime.now() - timedelta(minutes=interval)
            values = [data['quote'] for data in collection.find({'company': company, 'timestamp': {'$gt': time_ago}})]
            stats[f'avg_{interval}_minutes'] = sum(values) / len(values) if values else None
            stats[f'std_{interval}_minutes'] = statistics.stdev(values) if len(values) > 1 else None
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint per ottenere quanti valori di una certa azione sono stati ricevuti negli ultimi 10 secondi
@app.route('/api/count_last_10_seconds/<company>', methods=['GET'])
def count_last_10_seconds(company):
    try:
        ten_seconds_ago = datetime.now() - timedelta(seconds=10)
        count = collection.count_documents({'company': company, 'timestamp': {'$gt': ten_seconds_ago}})
        return jsonify({'company': company, 'count_last_10_seconds': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
