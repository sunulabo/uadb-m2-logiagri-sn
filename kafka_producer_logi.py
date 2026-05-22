# kafka_producer_logi.py — Simulateur IoT/ERP + pct_perte_reel (CORRIGÉ)
from kafka import KafkaProducer
import json, random, time, uuid
from datetime import datetime, date

PRODUITS = ['ARACHIDE','MANGUE','RIZ','TOMATE']
ZONES_PROD = ['CASAMANCE','BASSIN_ARACHIDIER','SINE_SALOUM','NIAYES']
ZONES_DEST = ['DAKAR','EXPORT','THIES','KAOLACK']

PARAMS = {
    'MANGUE': {'temp_max':12.0, 'duree_max_h':72, 'perte_base':0.05},
    'ARACHIDE': {'temp_max':30.0, 'duree_max_h':720, 'perte_base':0.02},
    'RIZ': {'temp_max':35.0, 'duree_max_h':4320, 'perte_base':0.01},
    'TOMATE': {'temp_max':10.0, 'duree_max_h':48, 'perte_base':0.08},
}

DISTANCES = {
    ('CASAMANCE','DAKAR'):490, ('CASAMANCE','EXPORT'):520,
    ('BASSIN_ARACHIDIER','DAKAR'):190, ('BASSIN_ARACHIDIER','KAOLACK'):50,
    ('SINE_SALOUM','DAKAR'):280, ('NIAYES','DAKAR'):45,
}

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def compute_pct_perte(produit: str, temp_c: float, delai_h: int) -> float:
    p = PARAMS[produit]
    depassement = max(0.0, temp_c - p['temp_max'])
    facteur_temp = depassement / 20.0
    facteur_duree = delai_h / p['duree_max_h']
    perte = p['perte_base'] + facteur_temp * 0.3 + facteur_duree * 0.2
    perte += random.gauss(0, 0.02)
    return round(max(0.0, min(1.0, perte)), 4)

def gen_transport() -> dict:
    produit = random.choice(PRODUITS)
    orig = random.choice(ZONES_PROD)
    dest = random.choice(ZONES_DEST)
    dist = DISTANCES.get((orig,dest), random.randint(80,600))
    delai = max(1, int(dist / random.uniform(40,80)))
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
        'pct_perte_reel': perte,
        'statut': 'EN_COURS',
    }

def gen_capteur(voyage_id: str) -> dict:
    return {
        'capteur_id': f'CPT{random.randint(1000,9999)}',
        'voyage_id': voyage_id,
        'timestamp_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'temperature_c': round(random.uniform(10, 42), 1),
        'latitude': round(random.uniform(12.3, 15.2), 4),
        'longitude': round(random.uniform(-17.2, -11.8), 4),
    }

if __name__ == '__main__':
    print('Simulateur Logi-Agri SN démarré...')
    while True:
        evt = gen_transport()
        producer.send('logi_raw', evt)
        print(f'→ {evt["produit"]} | {evt["zone_origine"]}→{evt["zone_dest"]} | perte={evt["pct_perte_reel"]:.2%}')
        for _ in range(3):
            producer.send('logi_capteurs', gen_capteur(evt['voyage_id']))
        producer.flush()
        time.sleep(3)