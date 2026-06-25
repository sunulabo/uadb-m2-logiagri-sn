#!/usr/bin/env python3
# kafka_producer_logi.py — Simulateur IoT/ERP + générateur pct_perte_reel
# ✓ CORRECTION v2 : génère pct_perte_reel pour l'entraînement ML

from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, date

# Configuration des produits et zones
PRODUITS = ['ARACHIDE', 'MANGUE', 'RIZ', 'TOMATE']
ZONES_PROD = ['CASAMANCE', 'BASSIN_ARACHIDIER', 'SINE_SALOUM', 'NIAYES']
ZONES_DEST = ['DAKAR', 'EXPORT', 'THIES', 'KAOLACK']

# Paramètres de conservation par produit
# temp_max : température tolérée (dépasser = risque de perte)
# duree_max_h : durée tolérée (dépasser = risque de perte)
# perte_base : taux de perte de base (même dans les conditions optimales)
PARAMS = {
    'MANGUE': {
        'temp_max': 12.0,
        'duree_max_h': 72,
        'perte_base': 0.05
    },
    'ARACHIDE': {
        'temp_max': 30.0,
        'duree_max_h': 720,
        'perte_base': 0.02
    },
    'RIZ': {
        'temp_max': 35.0,
        'duree_max_h': 4320,
        'perte_base': 0.01
    },
    'TOMATE': {
        'temp_max': 10.0,
        'duree_max_h': 48,
        'perte_base': 0.08
    },
}

# Distances entre zones (en km)
DISTANCES = {
    ('CASAMANCE', 'DAKAR'): 490,
    ('CASAMANCE', 'EXPORT'): 520,
    ('BASSIN_ARACHIDIER', 'DAKAR'): 190,
    ('BASSIN_ARACHIDIER', 'KAOLACK'): 50,
    ('SINE_SALOUM', 'DAKAR'): 280,
    ('NIAYES', 'DAKAR'): 45,
}

# Initialiser le producteur Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )
    print('✓ Connecté à Kafka')
except Exception as e:
    print(f'✗ Erreur de connexion Kafka : {e}')
    exit(1)


def compute_pct_perte(produit: str, temp_c: float, delai_h: int) -> float:
    """
    Calcule un pourcentage de perte réaliste basé sur :
    - Dépassement de température
    - Durée du transport
    - Taux de base du produit
    
    C'est la VARIABLE CIBLE pour l'entraînement du modèle ML
    """
    p = PARAMS[produit]
    
    # Facteur température : plus c'est chaud, plus c'est grave
    depassement = max(0.0, temp_c - p['temp_max'])
    facteur_temp = depassement / 20.0  # 20°C dépassement = +100% perte additionnelle
    
    # Facteur durée : plus c'est long, plus c'est grave
    facteur_duree = delai_h / p['duree_max_h']
    
    # Calcul final avec bruit réaliste
    perte = p['perte_base'] + (facteur_temp * 0.3) + (facteur_duree * 0.2)
    perte += random.gauss(0, 0.02)  # Bruit gaussien (variations naturelles)
    
    # Cliper entre 0% et 100%
    return round(max(0.0, min(1.0, perte)), 4)


def gen_transport() -> dict:
    """Génère un événement de transport avec tous les KPIs"""
    
    produit = random.choice(PRODUITS)
    orig = random.choice(ZONES_PROD)
    dest = random.choice(ZONES_DEST)
    dist = DISTANCES.get((orig, dest), random.randint(80, 600))
    delai = max(1, int(dist / random.uniform(40, 80)))
    temp = round(random.uniform(18, 40), 1)
    perte = compute_pct_perte(produit, temp, delai)
    
    return {
        'raw_entity_id': f'T_{uuid.uuid4().hex[:8].upper()}',
        'voyage_id': f'VY{random.randint(10000,99999)}',
        'produit': produit,
        'poids_kg': round(random.uniform(500, 15000), 1),
        'temp_moyenne_c': temp,
        'temp_max_tolere': PARAMS[produit]['temp_max'],
        'delai_prevu_h': delai,
        'date_depart': str(date.today()),
        'zone_origine': orig,
        'zone_dest': dest,
        'distance_km': float(dist),
        'cout_fcfa_km': round(random.uniform(250, 600), 1),
        'pct_perte_reel': perte,  # ← VARIABLE CIBLE ML
        'statut': 'EN_COURS',
    }


def gen_capteur(voyage_id: str) -> dict:
    """Génère un événement de capteur IoT"""
    return {
        'capteur_id': f'CPT{random.randint(1000,9999)}',
        'voyage_id': voyage_id,
        'timestamp_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'temperature_c': round(random.uniform(10, 42), 1),
        'latitude': round(random.uniform(12.3, 15.2), 4),
        'longitude': round(random.uniform(-17.2, -11.8), 4),
    }


if __name__ == '__main__':
    print('🚜 Simulateur Logi-Agri SN démarré...')
    print('   Topics générés : logi_raw (transport), logi_capteurs (IoT)')
    print('   Ctrl+C pour arrêter\n')
    
    event_count = 0
    try:
        while True:
            # Générer un transport
            evt_transport = gen_transport()
            producer.send('logi_raw', evt_transport)
            event_count += 1
            
            # Afficher le transport dans le terminal
            status = '🔴 CRITIQUE' if evt_transport['pct_perte_reel'] > 0.4 else \
                     '🟡 ALERTE' if evt_transport['pct_perte_reel'] > 0.2 else '🟢 OK'
            
            print(f'[{event_count:04d}] {evt_transport["produit"]:8} | '
                  f'{evt_transport["zone_origine"]:16} → {evt_transport["zone_dest"]:8} | '
                  f'Perte: {evt_transport["pct_perte_reel"]:.2%} | {status}')
            
            # Générer 3 événements capteurs pour ce transport
            for _ in range(3):
                evt_capteur = gen_capteur(evt_transport['voyage_id'])
                producer.send('logi_capteurs', evt_capteur)
            
            producer.flush()
            time.sleep(3)  # Un transport toutes les 3 secondes
            
    except KeyboardInterrupt:
        print('\n\n✓ Simulateur arrêté')
        producer.close()
        exit(0)