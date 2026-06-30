import subprocess
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s — %(message)s')
logger = logging.getLogger('HBaseShellSetup')

def run_hbase_shell(commands_str):
    """Exécute une série de commandes dans le HBase shell via Docker."""
    try:
        # On utilise subprocess pour envoyer les commandes au conteneur Docker
        process = subprocess.Popen(
            ['docker', 'exec', '-i', 'hbase', 'hbase', 'shell'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate(input=commands_str)
        
        if process.returncode != 0 and "ERROR" in stderr:
            logger.error(f"Erreur d'exécution:\n{stderr}")
            return False
        return True
    except Exception as e:
        logger.error(f"Erreur lors de l'appel à Docker: {e}")
        return False

def setup_logi_agri_tables():
    logger.info("Génération des commandes HBase Shell...")
    
    # Construction du script HBase Shell
    hbase_commands = """
    # Création du namespace (ignorera l'erreur s'il existe déjà)
    create_namespace 'logi'
    
    # Table: logi:transports
    # S'assure que la table est supprimée avant recréation (Optionnel, commenté ici)
    # disable 'logi:transports'
    # drop 'logi:transports'
    create 'logi:transports', {NAME => 'meta', VERSIONS => 1}, {NAME => 'kpi', VERSIONS => 5}, {NAME => 'position', VERSIONS => 10}
    
    # Table: logi:alertes
    create 'logi:alertes', {NAME => 'alerte', VERSIONS => 1, TTL => 172800}
    
    # Table: logi:stocks
    create 'logi:stocks', {NAME => 'inventaire', VERSIONS => 5}, {NAME => 'conditions', VERSIONS => 10}
    
    # Afficher la liste des tables
    list_namespace_tables 'logi'
    
    exit
    """
    
    logger.info("Exécution des commandes dans le conteneur HBase...")
    success = run_hbase_shell(hbase_commands)
    
    if success:
        logger.info("✅ Configuration HBase terminée avec succès via le Shell ! (Vérifiez les erreurs éventuelles si les tables existaient déjà)")
    else:
        logger.error("❌ Échec de la configuration.")

if __name__ == '__main__':
    setup_logi_agri_tables()
