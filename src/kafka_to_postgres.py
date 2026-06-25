#!/usr/bin/env python3
# kafka_to_postgres.py — Script d'ingestion Kafka vers PostgreSQL pour Grafana

import json
import psycopg2
from kafka import KafkaConsumer
import time

# --- Configuration ---
KAFKA_BROKER = 'localhost:29092'
TOPIC = 'logi_alertes'

DB_HOST = 'localhost'
DB_NAME = 'logiagri_db'
DB_USER = 'logiagri'
DB_PASS = 'logiagri2025'

def init_db():
    # Boucle de reconnexion au cas où Postgres est encore en cours de démarrage
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, port=5433, database=DB_NAME, user=DB_USER, password=DB_PASS)
            cursor = conn.cursor()
            
            # Création de la table pour stocker les KPIs de Grafana
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS logi_alertes (
                    id SERIAL PRIMARY KEY,
                    voyage_id VARCHAR(100),
                    produit VARCHAR(50),
                    zone_origine VARCHAR(100),
                    zone_dest VARCHAR(100),
                    temp_moyenne_c FLOAT,
                    cout_total_fcfa FLOAT,
                    risque_perte_score FLOAT,
                    statut_alerte VARCHAR(50),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            return conn, cursor
        except psycopg2.OperationalError:
            print("⏳ En attente de PostgreSQL...")
            time.sleep(3)

def main():
    print("🔄 Initialisation de la connexion PostgreSQL...")
    conn, cursor = init_db()
    print("✅ Connecté à PostgreSQL ! La table 'logi_alertes' est prête.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print(f"📡 Écoute en direct sur le topic Kafka '{TOPIC}'...")

    for msg in consumer:
        data = msg.value
        try:
            cursor.execute("""
                INSERT INTO logi_alertes (
                    voyage_id, produit, zone_origine, zone_dest, 
                    temp_moyenne_c, cout_total_fcfa, risque_perte_score, statut_alerte
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data.get('voyage_id', 'INCONNU'),
                data.get('produit', 'INCONNU'),
                data.get('zone_origine', 'INCONNU'),
                data.get('zone_dest', 'INCONNU'),
                data.get('temp_moyenne_c', 0.0),
                data.get('cout_total_fcfa', 0.0),
                data.get('risque_perte_score', 0.0),
                data.get('statut_alerte', 'INCONNU')
            ))
            conn.commit()
            print(f"📥 Donnée insérée: Voyage {data.get('voyage_id')} | Statut: {data.get('statut_alerte')} | Coût: {data.get('cout_total_fcfa')} FCFA")
        except Exception as e:
            print(f"❌ Erreur d'insertion: {e}")
            conn.rollback()

if __name__ == '__main__':
    main()
