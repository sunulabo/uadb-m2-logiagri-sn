# hbase_setup.py — Création des tables HBase pour Logi-Agri SN
import happybase
import logging

logger = logging.getLogger('HBaseSetup')

def create_logi_agri_tables():
    conn = happybase.Connection('localhost', port=9090, timeout=10000)
    conn.open()

    # Créer le namespace 'logi' d'abord
    try:
        conn.client.createNamespace(happybase.connection.Namespace(name='logi'))
        logger.info('Namespace logi créé')
    except Exception as e:
        logger.info(f'Namespace logi existe déjà ou erreur : {e}')

    tables_a_creer = {
        b'logi:transports': {
            'meta': {'max_versions': 1},
            'kpi': {'max_versions': 5},
            'position': {'max_versions': 10},
        },
        b'logi:alertes': {
            'alerte': {'max_versions': 1, 'time_to_live': 172800},
        },
        b'logi:stocks': {
            'inventaire': {'max_versions': 5},
            'conditions': {'max_versions': 10},
        },
    }

    existantes = [t.decode() for t in conn.tables()]
    for nom_bytes, families in tables_a_creer.items():
        nom = nom_bytes.decode()
        if nom in existantes:
            logger.info(f'Table {nom} déjà existante')
        else:
            conn.create_table(nom_bytes, families)
            logger.info(f'Table {nom} créée')
    conn.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    create_logi_agri_tables()