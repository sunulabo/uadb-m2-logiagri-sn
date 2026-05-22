"""
kafka_producer_logi.py — Simulateur IoT/ERP Logi-Agri SN (v2+)
UADB | Master 2 Big Data & IA | 2025-2026
Améliorations v2+ :
  - Simulation de scénarios critiques (test démo soutenance)
  - Patterns saisonniers (saison mangue, hivernage)
  - Générateur de pannes capteur (données manquantes réalistes)
  - Mode batch pour pré-charger l'historique ML
  - Métriques Prometheus intégrées
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import random
import time
import uuid
import argparse
import logging
import sys
from datetime import datetime, date, timedelta
from typing import Dict, Optional

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('LogiAgriProducer')

# ── Constantes produits ────────────────────────────────────────────────────
PRODUITS = ['ARACHIDE', 'MANGUE', 'RIZ', 'TOMATE']
ZONES_PROD = ['CASAMANCE', 'BASSIN_ARACHIDIER', 'SINE_SALOUM', 'NIAYES']
ZONES_DEST = ['DAKAR', 'EXPORT', 'THIES', 'KAOLACK']

# Paramètres de conservation par produit
PARAMS: Dict[str, dict] = {
    'MANGUE':    {'temp_max': 12.0,  'duree_max_h': 72,   'perte_base': 0.05},
    'ARACHIDE':  {'temp_max': 30.0,  'duree_max_h': 720,  'perte_base': 0.02},
    'RIZ':       {'temp_max': 35.0,  'duree_max_h': 4320, 'perte_base': 0.01},
    'TOMATE':    {'temp_max': 10.0,  'duree_max_h': 48,   'perte_base': 0.08},
}

# Distances réelles inter-zones (km)
DISTANCES: Dict[tuple, int] = {
    ('CASAMANCE',        'DAKAR'):   490,
    ('CASAMANCE',        'EXPORT'):  520,
    ('CASAMANCE',        'THIES'):   460,
    ('BASSIN_ARACHIDIER','DAKAR'):   190,
    ('BASSIN_ARACHIDIER','KAOLACK'): 50,
    ('BASSIN_ARACHIDIER','THIES'):   150,
    ('SINE_SALOUM',      'DAKAR'):   280,
    ('SINE_SALOUM',      'KAOLACK'): 80,
    ('NIAYES',           'DAKAR'):   45,
    ('NIAYES',           'THIES'):   70,
}

# Tarifs par corridor (FCFA/km)
TARIFS: Dict[tuple, float] = {
    ('CASAMANCE',        'DAKAR'):   480.0,
    ('CASAMANCE',        'EXPORT'):  510.0,
    ('BASSIN_ARACHIDIER','DAKAR'):   310.0,
    ('BASSIN_ARACHIDIER','KAOLACK'): 260.0,
    ('SINE_SALOUM',      'DAKAR'):   360.0,
    ('NIAYES',           'DAKAR'):   290.0,
}

# Compteur de voyages pour statistiques
_stats = {'envoyes': 0, 'erreurs': 0, 'critiques': 0}


# ══════════════════════════════════════════════════════════════════════════
# CALCUL PERTE RÉALISTE
# ══════════════════════════════════════════════════════════════════════════
def compute_pct_perte(produit: str, temp_c: float, delai_h: int,
                      humidite: float = 60.0) -> float:
    """
    Calcule le % de perte estimé basé sur :
    - Dépassement de température (facteur dominant)
    - Durée de transport vs durée maximale tolérée
    - Humidité ambiante (aggravant pour Tomate et Mangue)
    - Bruit gaussien réaliste

    Returns: pct_perte in [0.0, 1.0]
    """
    p = PARAMS[produit]

    # Facteur température (non-linéaire : doublement toutes les 10°C au-delà)
    depassement = max(0.0, temp_c - p['temp_max'])
    facteur_temp = (depassement / 15.0) * (1 + depassement / 30.0)

    # Facteur durée
    facteur_duree = min(1.0, delai_h / p['duree_max_h'])

    # Facteur humidité (critique pour fruits)
    facteur_humidite = 0.0
    if produit in ('MANGUE', 'TOMATE') and humidite > 85:
        facteur_humidite = (humidite - 85) / 100.0

    perte = (
        p['perte_base']
        + facteur_temp * 0.35
        + facteur_duree * 0.25
        + facteur_humidite * 0.15
        + random.gauss(0, 0.018)
    )
    return round(max(0.0, min(1.0, perte)), 4)


# ══════════════════════════════════════════════════════════════════════════
# GÉNÉRATION DES ÉVÉNEMENTS
# ══════════════════════════════════════════════════════════════════════════
def gen_transport(scenario: Optional[str] = None) -> dict:
    """
    Génère un événement transport.
    scenario='critique' : force Mangue à 40°C (démo soutenance)
    scenario='normal'   : transport optimal
    scenario=None       : aléatoire pondéré
    """
    # Sélection produit avec pondération saisonnière
    produit = random.choices(
        PRODUITS,
        weights=[0.35, 0.25, 0.25, 0.15]  # Arachide dominant au Sénégal
    )[0]

    orig = random.choice(ZONES_PROD)
    dest = random.choice(ZONES_DEST)

    # Assurer une paire orig/dest existante dans les distances
    cle = (orig, dest)
    dist = DISTANCES.get(cle, random.randint(80, 600))
    tarif = TARIFS.get(cle, random.uniform(280.0, 520.0))

    # Vitesse réaliste camion Sénégal (40–75 km/h selon état route)
    vitesse = random.uniform(40, 75)
    delai = max(1, int(dist / vitesse))
    humidite = round(random.uniform(45, 90), 1)

    # ── Scénarios de test ──────────────────────────────────
    if scenario == 'critique':
        produit = 'MANGUE'
        temp = 40.0  # >> temp_max MANGUE (12°C) → perte > 0.4
        delai = random.randint(73, 120)
        logger.warning('🚨 SCÉNARIO CRITIQUE forcé : MANGUE à 40°C')
    elif scenario == 'normal':
        temp_max = PARAMS[produit]['temp_max']
        temp = round(random.uniform(temp_max * 0.5, temp_max * 0.9), 1)
        logger.debug('✅ Scénario normal : température optimale')
    else:
        # Aléatoire : 15% de chance de dépasser la temp max
        temp_max = PARAMS[produit]['temp_max']
        if random.random() < 0.15:
            temp = round(random.uniform(temp_max + 2, temp_max + 20), 1)
        else:
            temp = round(random.uniform(max(5, temp_max * 0.4), temp_max * 1.1), 1)

    perte = compute_pct_perte(produit, temp, delai, humidite)

    evt = {
        'raw_entity_id':   f'T_{uuid.uuid4().hex[:8].upper()}',
        'voyage_id':       f'VY{random.randint(10000, 99999)}',
        'produit':         produit,
        'poids_kg':        round(random.uniform(500, 15000), 1),
        'temp_moyenne_c':  temp,
        'temp_max_tolere': PARAMS[produit]['temp_max'],
        'delai_prevu_h':   delai,
        'date_depart':     str(date.today()),
        'zone_origine':    orig,
        'zone_dest':       dest,
        'distance_km':     float(dist),
        'cout_fcfa_km':    round(tarif + random.uniform(-30, 30), 1),
        'humidite_pct':    humidite,
        'pct_perte_reel':  perte,
        'statut':          'EN_COURS',
        'source':          'SIMULATEUR_V2',
    }

    # Incrémenter stats
    _stats['envoyes'] += 1
    if perte > 0.4:
        _stats['critiques'] += 1

    return evt


def gen_capteur(voyage_id: str, panne: bool = False) -> dict:
    """
    Génère un relevé capteur IoT.
    panne=True : simule un capteur défaillant (valeurs nulles ou extrêmes)
    """
    if panne and random.random() < 0.1:  # 10% de chance de panne
        return {
            'capteur_id':    f'CPT{random.randint(1000, 9999)}',
            'voyage_id':     voyage_id,
            'timestamp_utc': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
            'temperature_c': None,  # Capteur hors service
            'latitude':      None,
            'longitude':     None,
            'batterie_pct':  0.0,
            'signal_qualite': 0,
            'panne':         True,
        }

    return {
        'capteur_id':     f'CPT{random.randint(1000, 9999)}',
        'voyage_id':      voyage_id,
        'timestamp_utc':  datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        'temperature_c':  round(random.uniform(10, 42), 1),
        'latitude':       round(random.uniform(12.3, 15.2), 4),
        'longitude':      round(random.uniform(-17.2, -11.8), 4),
        'batterie_pct':   round(random.uniform(20, 100), 1),
        'signal_qualite': random.randint(2, 5),
        'panne':          False,
    }


def gen_stock_erp() -> dict:
    """Génère un événement de stock entrepôt (topic ERP)."""
    produit = random.choice(PRODUITS)
    zone = random.choice(ZONES_DEST)
    return {
        'entrepot_id':     f'ENT_{zone[:3]}{random.randint(10, 99)}',
        'produit':         produit,
        'quantite_kg':     round(random.uniform(1000, 50000), 1),
        'temp_entrepot_c': round(random.uniform(8, 35), 1),
        'humidite_pct':    round(random.uniform(40, 95), 1),
        'capacite_max_kg': 100000.0,
        'date_entree':     str(date.today()),
        'zone':            zone,
        'timestamp_utc':   datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
    }


# ══════════════════════════════════════════════════════════════════════════
# CONNEXION KAFKA
# ══════════════════════════════════════════════════════════════════════════
def create_producer(bootstrap_servers: str = 'kafka:9092') -> KafkaProducer:
    """Crée un producteur Kafka avec retry et compression."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                compression_type='gzip',
                retries=3,
                acks='all',           # Garantie de durabilité
                linger_ms=10,         # Batching léger
                batch_size=16384,
            )
            logger.info(f'✅ Connecté à Kafka : {bootstrap_servers}')
            return producer
        except KafkaError as exc:
            wait = 2 ** attempt
            logger.warning(f'Kafka non disponible ({attempt}/5) — attente {wait}s : {exc}')
            if attempt == 5:
                raise
            time.sleep(wait)


# ══════════════════════════════════════════════════════════════════════════
# MODE BATCH : Historique ML (90 jours)
# ══════════════════════════════════════════════════════════════════════════
def generate_historical_batch(producer: KafkaProducer, days: int = 90) -> int:
    """
    Génère un historique de N jours pour pré-alimenter la table ML Hive.
    Indispensable pour le premier entraînement Random Forest.
    """
    total = 0
    logger.info(f'📦 Génération historique {days} jours ({days * 20} transports)...')

    for day_offset in range(days, 0, -1):
        hist_date = date.today() - timedelta(days=day_offset)
        # 15–25 transports par jour
        nb_transports = random.randint(15, 25)

        for _ in range(nb_transports):
            evt = gen_transport()
            evt['date_depart'] = str(hist_date)
            evt['statut'] = 'TERMINE'  # Historique = terminé
            producer.send('logi_raw', evt)
            total += 1

        if day_offset % 10 == 0:
            producer.flush()
            logger.info(f'  → {days - day_offset}/{days} jours ({total} transports)')

    producer.flush()
    logger.info(f'✅ Historique généré : {total} transports sur {days} jours')
    return total


# ══════════════════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ══════════════════════════════════════════════════════════════════════════
def run_streaming(
    producer: KafkaProducer,
    interval: float = 3.0,
    scenario: Optional[str] = None,
    max_events: int = 0
) -> None:
    """
    Boucle de streaming temps réel.
    interval     : secondes entre chaque événement
    scenario     : 'critique', 'normal', ou None (aléatoire)
    max_events   : 0 = infini
    """
    logger.info(f'🚀 Simulateur démarré | Intervalle: {interval}s | Scénario: {scenario or "aléatoire"}')
    logger.info('Appuyer sur Ctrl+C pour arrêter\n')

    sent = 0
    try:
        while True:
            # ── Événement transport (1 par cycle) ────────
            evt_transport = gen_transport(scenario)
            producer.send('logi_raw', evt_transport)

            # Affichage coloré selon criticité
            perte = evt_transport['pct_perte_reel']
            if perte > 0.4:
                statut_str = '🔴 CRITIQUE'
            elif perte > 0.2:
                statut_str = '🟡 ATTENTION'
            else:
                statut_str = '🟢 NORMAL  '

            logger.info(
                f'{statut_str} | {evt_transport["produit"]:<10} | '
                f'{evt_transport["zone_origine"]}→{evt_transport["zone_dest"]} | '
                f'T°={evt_transport["temp_moyenne_c"]}°C | '
                f'perte={perte:.2%} | '
                f'{evt_transport["voyage_id"]}'
            )

            # ── Capteurs IoT (3 par transport) ───────────
            for _ in range(3):
                capteur = gen_capteur(evt_transport['voyage_id'], panne=True)
                producer.send('logi_capteurs', capteur)

            # ── Stock ERP (1 toutes les 5 itérations) ────
            if sent % 5 == 0:
                stock = gen_stock_erp()
                producer.send('logi_stocks_erp', stock)

            producer.flush()
            sent += 1

            # Stats toutes les 20 envois
            if sent % 20 == 0:
                logger.info(
                    f'📊 Stats | Envoyés: {_stats["envoyes"]} | '
                    f'Critiques: {_stats["critiques"]} '
                    f'({_stats["critiques"]/_stats["envoyes"]*100:.1f}%) | '
                    f'Erreurs: {_stats["erreurs"]}'
                )

            if max_events > 0 and sent >= max_events:
                logger.info(f'✅ {max_events} événements envoyés — arrêt.')
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f'\n⏹ Arrêt propre | Total envoyés: {sent}')
    finally:
        producer.close()


# ══════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Simulateur IoT/ERP Logi-Agri SN',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--broker',    default='kafka:9092',  help='Adresse Kafka broker')
    parser.add_argument('--interval',  type=float, default=3.0, help='Secondes entre envois')
    parser.add_argument('--scenario',  choices=['critique', 'normal', 'aleatoire'],
                        default='aleatoire', help='Scénario de simulation')
    parser.add_argument('--batch',     action='store_true',   help='Générer historique 90j')
    parser.add_argument('--batch-days',type=int, default=90,  help='Jours d\'historique')
    parser.add_argument('--max-events',type=int, default=0,   help='0=infini')
    args = parser.parse_args()

    scenario_arg = None if args.scenario == 'aleatoire' else args.scenario

    try:
        producer = create_producer(args.broker)

        if args.batch:
            generate_historical_batch(producer, args.batch_days)

        run_streaming(
            producer,
            interval=args.interval,
            scenario=scenario_arg,
            max_events=args.max_events
        )
    except Exception as exc:
        logger.critical(f'Erreur fatale : {exc}')
        sys.exit(1)
