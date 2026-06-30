# Logi-Agri SN

Logistique Agricole & IoT Streaming - Master 2 Big Data UADB 2025-2026 - Equipe 03

Projet realise par Cheikh Ahmadou Ka et Abdou Zatadini.

## Objectif

Logi-Agri SN est une plateforme de streaming Big Data pour suivre des flux logistiques agricoles en temps reel.

Le pipeline simule des evenements de transport et de capteurs IoT, les traite avec Spark Streaming, publie des alertes dans Kafka, puis expose les resultats dans deux dashboards :

- **Streamlit** : dashboard temps reel connecte directement a Kafka.
- **Grafana** : dashboard de monitoring connecte a PostgreSQL.

## Architecture

```text
Producteur Python
  -> Kafka topics: logi_raw, logi_capteurs
  -> Spark Streaming
  -> Kafka topics: logi_alertes, logi_temp_agg
  -> kafka_to_postgres.py
  -> PostgreSQL
  -> Grafana

Kafka logi_alertes
  -> Streamlit dashboard
```

## Prerequis

Verifiez Docker :

```bash
docker --version
docker compose version
```

Installez les dependances Python :

```bash
pip install -r requirements.txt
```

Spark local doit utiliser Java 17. Sur macOS :

```bash
brew install --cask temurin@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH
java -version
```

La commande `java -version` doit afficher une version 17.

## Structure utile

```text
docker/
  docker-compose.yml          Infrastructure Docker
  config/prometheus.yml       Configuration Prometheus

config/grafana/
  datasources/                Datasources Grafana provisionnees
  dashboards/                 Dashboards Grafana provisionnes

src/
  kafka_producer_logi.py      Simulateur IoT/ERP
  streaming_logistics.py      Pipeline Spark Streaming
  kafka_to_postgres.py        Ingestion Kafka vers PostgreSQL pour Grafana
  dashboard.py                Dashboard Streamlit temps reel

scripts/
  init_grafana_db.sql         Initialisation de la table logi_alertes
```

## Demarrage de l'infrastructure

Depuis le dossier `docker` :

```bash
cd docker
docker compose up -d
```

Verifier les conteneurs :

```bash
docker compose ps
```

Voir les logs :

```bash
docker compose logs -f
```

## Interfaces web

| Service | URL | Identifiants |
| --- | --- | --- |
| Spark Master | http://localhost:8080 | - |
| NiFi | http://localhost:8081 | - |
| Airflow | http://localhost:8082 | generes au demarrage par Airflow standalone |
| HBase | http://localhost:16010 | - |
| Prometheus | http://localhost:9091 | - |
| Grafana | http://localhost:3000 | admin / logiagri2025 |
| Streamlit | http://localhost:8501 | - |

## Lancer le pipeline complet

Ouvrez plusieurs terminaux depuis la racine du projet.

### 1. Demarrer Docker

```bash
cd docker
docker compose up -d
cd ..
```

### 2. Lancer le simulateur Kafka

```bash
python3 src/kafka_producer_logi.py
```

Le script produit des messages dans :

- `logi_raw`
- `logi_capteurs`

### 3. Lancer Spark Streaming

Dans un autre terminal :

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH
python3 src/streaming_logistics.py
```

Le script lit `logi_raw` et `logi_capteurs`, calcule les indicateurs logistiques, puis publie :

- `logi_alertes`
- `logi_temp_agg`

### 4. Alimenter PostgreSQL pour Grafana

Dans un autre terminal :

```bash
python3 src/kafka_to_postgres.py
```

Le script consomme `logi_alertes` et insere les donnees dans la table PostgreSQL `logi_alertes`.

### 5. Lancer le dashboard Streamlit

Dans un autre terminal :

```bash
streamlit run src/dashboard.py
```

Ouvrir ensuite :

```text
http://localhost:8501
```

## Dashboards

### Grafana

Grafana est provisionne automatiquement avec :

- datasource PostgreSQL : `LogiAgri PostgreSQL`
- datasource Prometheus : `Prometheus`
- dashboard : `Logi-Agri SN - Monitoring logistique`

Le dashboard Grafana lit PostgreSQL. Si les graphiques sont vides, verifiez que `src/kafka_to_postgres.py` est lance et que Spark publie bien dans `logi_alertes`.

### Streamlit

Le dashboard Streamlit lit Kafka directement depuis le topic `logi_alertes`.

Il affiche :

- nombre de transports suivis
- nombre d'alertes critiques
- cout logistique cumule
- volume transporte
- repartition des statuts d'alerte
- cout par produit
- derniers mouvements logistiques

## Variables d'environnement utiles

`streaming_logistics.py` :

```bash
export KAFKA_BROKERS=localhost:29092
export LOGI_SECRET_SALT=logi_agri_sn_2025_uadb_secret
```

`kafka_to_postgres.py` :

```bash
export KAFKA_BROKER=localhost:29092
export KAFKA_TOPIC=logi_alertes
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5433
export POSTGRES_DB=logiagri_db
export POSTGRES_USER=logiagri
export POSTGRES_PASSWORD=logiagri2025
```

## Arreter l'infrastructure

Depuis le dossier `docker` :

```bash
docker compose down
```

Supprimer aussi les volumes et les donnees :

```bash
docker compose down -v
```

## Depannage

### Docker daemon non lance

Erreur possible :

```text
failed to connect to the docker API
```

Solution : demarrer Docker Desktop, puis relancer :

```bash
cd docker
docker compose up -d
```

### Erreur Spark JAVA_GATEWAY_EXITED

Erreur possible :

```text
PySparkRuntimeError: [JAVA_GATEWAY_EXITED]
```

Solution : utiliser Java 17 dans le terminal qui lance Spark :

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH
python3 src/streaming_logistics.py
```

### Fichier Python introuvable

Erreur possible :

```text
can't open file 'kafka_to_postgres.py'
```

Depuis la racine du projet, utiliser :

```bash
python3 src/kafka_to_postgres.py
```

### Port deja utilise

Exemple pour trouver un processus :

```bash
lsof -i :3000
```

Puis arreter le processus concerne ou changer le port dans `docker/docker-compose.yml`.

### Logs d'un service

```bash
cd docker
docker compose logs -f kafka
docker compose logs -f grafana
docker compose logs -f postgres
```

## Services Docker inclus

- ZooKeeper `confluentinc/cp-zookeeper:7.4.0`
- Kafka `confluentinc/cp-kafka:7.4.0`
- NiFi `apache/nifi:1.23.2`
- HBase `harisekhon/hbase:2.1`
- Hive Metastore `apache/hive:3.1.3`
- HiveServer2 `apache/hive:3.1.3`
- Spark master/worker `apache/spark:latest`
- Airflow `apache/airflow:2.7.3`
- PostgreSQL `postgres:15`
- Prometheus `prom/prometheus:v2.47.0`
- Grafana `grafana/grafana:10.2.0`

## Notes

- Les volumes Docker `postgres_data`, `prometheus_data` et `grafana_data` conservent les donnees.
- La table PostgreSQL `logi_alertes` est creee automatiquement au premier demarrage du conteneur PostgreSQL.
- Les dashboards Grafana sont charges depuis `config/grafana/dashboards`.
- Les datasources Grafana sont chargees depuis `config/grafana/datasources`.
