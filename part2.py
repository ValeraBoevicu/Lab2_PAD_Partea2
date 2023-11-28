from pymongo import MongoClient
from flask import Flask, request, jsonify
import random
import argparse  # Adăugată pentru a procesa argumentele de linie de comandă

app = Flask(__name__)

# Conectarea la MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['warehouse_db']
collection = db['data']

# Funcția de load balancing(fictiva)
def load_balance():
    servers = ["server1", "server2", "server3"]
    selected_server = random.choice(servers)
    return selected_server

# Funcția de smart-proxy
def smart_proxy(data_key):
    # Verificăm dacă datele sunt în cache (în cazul nostru, stocarea în memorie temporară)
    if data_key in cache:
        return cache[data_key], 'cache'

    # Dacă datele nu sunt în cache, le extragem din MongoDB
    server = load_balance()
    data_from_db = collection.find_one({'key': data_key})

    # Verificăm dacă am găsit date în MongoDB
    if data_from_db is not None and 'value' in data_from_db:
        # Salvăm datele în cache pentru viitoarele cereri
        cache[data_key] = data_from_db['value']
        return data_from_db['value'], 'database'
    else:
        return 'Data not found', 'database'

# Ruta pentru nodul proxy
@app.route('/proxy', methods=['GET', 'POST'])
def proxy():
    if request.method == 'GET':
        data_key = request.args.get('key', 'example_key')
        data, source = smart_proxy(data_key)
        return jsonify({'data': data, 'source': source})
    elif request.method == 'POST':
        posted_data = request.json

        # Verificăm dacă cheia și valoarea sunt prezente în datele trimise
        if 'key' in posted_data and 'value' in posted_data:
            # Actualizăm sau adăugăm datele în MongoDB
            data_key = posted_data['key']
            data_value = posted_data['value']
            collection.update_one({'key': data_key}, {'$set': {'value': data_value}}, upsert=True)

            return jsonify({'message': f'Data for key {data_key} updated/created successfully'})
        else:
            return jsonify({'error': 'Invalid data format for POST request'})

if __name__ == '__main__':
    # Inițializarea cache-ului (în cazul nostru, un dicționar în care stocăm temporar datele)
    cache = {}

    # Procesarea argumentelor de linie de comandă pentru a primi portul din exterior
    parser = argparse.ArgumentParser(description="Run the Flask application.")
    parser.add_argument("--port", type=int, default=5000, help="Port number")
    args = parser.parse_args()

    # Pornirea serverului Flask pe portul specificat
    app.run(port=args.port, debug=True)


# load-balancing(eureca pentru multi baze de date si docker) implementat + reverse proxy
# part 1 - procesarea concurenta a cererilor(threaduri), notiunea de thread safe = Facut