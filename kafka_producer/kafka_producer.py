from kafka import KafkaProducer
from kafka.vendor import six
import time
import csv
import json

# Connexion au broker Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Nom du topic Kafka
topic_name = 'population_data'

# Fichier source
input_file = './input_data/total-population.csv'

# Lire le fichier CSV
with open(input_file, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Ignorer l'en-tête
    batch = []

    for row in reader:
        # Ajouter la ligne au lot
        batch.append({header[i]: value for i, value in enumerate(row)})
        
        # Si le lot contient 100 lignes, envoyez-les à Kafka
        if len(batch) == 100:
            for record in batch:
                producer.send(topic_name, value=json.dumps(record).encode('utf-8'))
            batch = []
            time.sleep(10)  # Pause de 10 secondes

    # Envoyer les lignes restantes
    for record in batch:
        producer.send(topic_name, value=json.dumps(record).encode('utf-8'))
