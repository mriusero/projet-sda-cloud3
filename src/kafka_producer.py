from kafka import KafkaProducer
import json
import random

producer = KafkaProducer(
    bootstrap_servers="broker",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conforts = [
    {"confort": "high", "prix_base_per_km": 0.5},
    {"confort": "medium", "prix_base_per_km": 0.3},
    {"confort": "low", "prix_base_per_km": 0.2}
]

clusters = [
    {"client": {"lat": 48.8566, "lon": 2.3522}, "driver": {"lat": 40.4168, "lon": -3.7038}},
    {"client": {"lat": 40.7128, "lon": -74.0060}, "driver": {"lat": 41.8781, "lon": -87.6298}},
    {"client": {"lat": 35.6895, "lon": 139.6917}, "driver": {"lat": 35.1814, "lon": 136.9066}},
    {"client": {"lat": 52.5200, "lon": 13.4050}, "driver": {"lat": 41.9028, "lon": 12.4964}},
    {"client": {"lat": -12.0464, "lon": -77.0428}, "driver": {"lat": -33.4489, "lon": -70.6483}},
    {"client": {"lat": 19.4326, "lon": -99.1332}, "driver": {"lat": 20.6597, "lon": -103.3496}},
    {"client": {"lat": -33.9249, "lon": 18.4241}, "driver": {"lat": -4.4419, "lon": 15.2663}},
    {"client": {"lat": 25.2048, "lon": 55.2708}, "driver": {"lat": 33.3152, "lon": 44.3661}}
]


def generer_messages(nombre_messages):
    messages = []
    for _ in range(nombre_messages):
        confort = random.choice(conforts)
        cluster_data = random.choice(clusters)

        message = {
            "data": [
                {
                    "confort": confort["confort"],
                    "prix_base_per_km": confort["prix_base_per_km"],
                    "properties-client": {
                        "longitude": cluster_data["client"]["lon"],
                        "latitude": cluster_data["client"]["lat"],
                        "nomclient": "FALL",
                        "telephoneClient": "060786575"
                    },
                    "properties-driver": {
                        "longitude": cluster_data["driver"]["lon"],
                        "latitude": cluster_data["driver"]["lat"],
                        "nomDriver": "DIOP",
                        "telephoneDriver": "0760786575"
                    }
                }
            ]
        }

        producer.send('topic_name', value=message)
        messages.append(message)

    producer.flush()

    print(f"{nombre_messages} messages envoyés avec succès.")
    return messages

nombre_messages = 10000
messages_generes = generer_messages(nombre_messages)