# hbase_setup.py — Création des tables HBase pour Logi-Agri SN
import happybase
import logging
import time
import socket

logger = logging.getLogger('HBaseSetup')


def wait_for_thrift(host='localhost', port=9090, retries=10, delay=5):
    """Attendre que le serveur Thrift HBase soit prêt."""
    for i in range(retries):
        try:
<<<<<<< HEAD
            logger.info(f'Tentative de connexion à HBase (tentative {retry_count+1}/{max_retries})...')
            conn = happybase.Connection('hbase', port=9091, timeout=10000)
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
    
=======
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            logger.info(f'Thrift server disponible sur {host}:{port}')
            return True
        except (socket.error, ConnectionRefusedError):
            logger.warning(f'Tentative {i+1}/{retries} — Thrift pas encore prêt, attente {delay}s...')
            time.sleep(delay)
    return False


def create_logi_agri_tables():
    # 1. Vérifier que le Thrift server est prêt
    if not wait_for_thrift():
        raise RuntimeError("Impossible de joindre HBase Thrift server sur localhost:9090")

    # 2. Connexion happybase (Thrift 1 uniquement, pas de namespace API)
    conn = happybase.Connection(
        host='localhost',
        port=9090,
        timeout=30000,        # 30s timeout
        transport='framed',  # 'buffered' ou 'framed' selon la config HBase
        protocol='binary',
    )

>>>>>>> 8c2cc91f (final:infrastructure fonctionnel)
    try:
        conn.open()
        logger.info("Connexion HBase établie")

        # ⚠️  Les namespaces doivent être créés via HBase Shell au préalable :
        #     docker exec -it hbase hbase shell -e "create_namespace 'logi'"
        # 
        # Note : happybase/Thrift1 ne supporte pas createNamespace()

        tables_a_creer = {
            b'logi:transports': {
                b'meta':     dict(max_versions=1),
                b'kpi':      dict(max_versions=5),
                b'position': dict(max_versions=10),
            },
            b'logi:alertes': {
                b'alerte': dict(max_versions=1, time_to_live=172800),
            },
            b'logi:stocks': {
                b'inventaire':  dict(max_versions=5),
                b'conditions':  dict(max_versions=10),
            },
        }

        existantes = {t.decode() for t in conn.tables()}
        logger.info(f"Tables existantes : {existantes or '(aucune)'}")

        for nom_bytes, families in tables_a_creer.items():
            nom = nom_bytes.decode()
            if nom in existantes:
                logger.info(f'  [SKIP] Table {nom} déjà existante')
            else:
                conn.create_table(nom_bytes, families)
                logger.info(f'  [OK]   Table {nom} créée')

    except Exception as e:
        logger.error(f"Erreur lors de la création des tables : {e}")
        raise
    finally:
        conn.close()
        logger.info("Connexion fermée")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s — %(message)s'
    )
    create_logi_agri_tables()