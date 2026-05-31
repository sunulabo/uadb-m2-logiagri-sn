#!/usr/bin/env python3
# hbase_setup.py — Création des tables HBase pour Logi-Agri SN
# À exécuter UNE SEULE FOIS après docker compose up

import happybase
import logging
import time
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('HBaseSetup')

def create_logi_agri_tables():
    """
    Crée les 3 tables HBase nécessaires :
    - logi:transports : flux temps réel des transports
    - logi:alertes : alertes de pertes imminentes
    - logi:stocks : stocks en entrepôt
    """
    
    # Paramètres de connexion
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f'Tentative de connexion à HBase (tentative {retry_count+1}/{max_retries})...')
            conn = happybase.Connection('hbase', port=16010, timeout=10000)
            conn.open()
            logger.info('✓ Connexion à HBase établie')
            break
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f'Impossible de se connecter à HBase après {max_retries} tentatives')
                logger.error(f'Erreur : {e}')
                sys.exit(1)
            logger.warning(f'Connexion échouée, attente 5s...')
            time.sleep(5)
    
    # Définition des tables à créer
    tables_a_creer = {
        # Flux temps réel des transports en cours
        b'logi:transports': {
            b'meta': {'max_versions': 1},        # voyage_id, produit, zone
            b'kpi': {'max_versions': 5},         # cout_tonne, taux_perte, statut
            b'position': {'max_versions': 10},   # lat, lon, temp_actuelle
        },
        
        # Alertes de pertes imminentes (TTL 48h)
        b'logi:alertes': {
            b'alerte': {'max_versions': 1, 'time_to_live': 172800},
        },
        
        # Stock entrepôts temps réel
        b'logi:stocks': {
            b'inventaire': {'max_versions': 5},
            b'conditions': {'max_versions': 10},  # temp, humidite
        },
    }
    
    try:
        # Récupérer les tables existantes
        existantes = [t.decode() for t in conn.tables()]
        logger.info(f'Tables existantes : {existantes}')
        
        # Créer les tables manquantes
        for nom_bytes, families in tables_a_creer.items():
            nom = nom_bytes.decode()
            if nom in existantes:
                logger.info(f'  ⚠ Table {nom} déjà existante (skipped)')
            else:
                conn.create_table(nom_bytes, families)
                logger.info(f'  ✓ Table {nom} créée')
        
        conn.close()
        logger.info('✓ Initialisation HBase réussie')
        return True
        
    except Exception as e:
        logger.error(f'Erreur lors de la création des tables : {e}')
        conn.close()
        return False


if __name__ == '__main__':
    success = create_logi_agri_tables()
    sys.exit(0 if success else 1)