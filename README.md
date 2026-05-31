# uadb-m2-logiagri-sn
Logistique Agricole &amp; IoT Streaming — Master 2 Big Data UADB 2025-2026
# 🚀 Logi-Agri SN — Docker Infrastructure Setup

## ✅ Prérequis installés

```bash
# Vérifiez les versions
docker --version
docker compose version
```

## 📁 Structure des répertoires (créée automatiquement)

```
docker/
├── docker-compose.yml          # Configuration principale
├── .env                        # Variables d'environnement
├── config/
│   ├── prometheus.yml          # Configuration Prometheus
│   └── grafana/
│       ├── datasources/
│       │   └── prometheus.yml  # Data source Grafana
│       └── dashboards/
│           └── dashboards.yml  # Tableau de bord config
├── dags/                       # DAGs Airflow (à ajouter)
├── models/                     # Modèles ML (à ajouter)
├── logs/
│   └── airflow/               # Logs Airflow
├── nifi_templates/            # Templates NiFi
└── scripts/
    └── init_mlflow_db.sql     # Initialisation MLflow
```

## 🎯 Lancement de l'infrastructure

### 1️⃣ Démarrer tous les services

```bash
cd docker
docker compose up -d
```

### 2️⃣ Vérifier l'état des services

```bash
docker compose ps
```

### 3️⃣ Voir les logs en temps réel

```bash
docker compose logs -f
```

### 4️⃣ Accéder aux interfaces web

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8082 | admin / logiagri2025 |cheikh cheikh123
| NiFi | http://localhost:8081 | admin / logiagri2025! | admin admin
| Kafka UI | http://localhost:8085 | - |
| Spark Master | http://localhost:8080 | - |
| MLflow | http://localhost:5000 | - |
| Grafana | http://localhost:3000 | admin / logiagri2025 |
| Prometheus | http://localhost:9091 | - |

## 🛑 Arrêter l'infrastructure

```bash
docker compose down
```

## 🧹 Nettoyer complètement (WARNING: supprime les données)

```bash
docker compose down -v
```

## 🔧 Troubleshooting

### Port déjà utilisé

```bash
# Trouver le processus (macOS/Linux)
lsof -i :9090

# Tuer le processus
kill -9 <PID>
```

### Logs d'un service spécifique

```bash
docker compose logs -f airflow-webserver
docker compose logs -f kafka
docker compose logs -f spark-master
```

### Redémarrer un service

```bash
docker compose restart airflow-webserver
```

### Reconstruire les images

```bash
docker compose build --no-cache
```

## 📊 Services inclus

- **Airflow 2.7.3** — Orchestration workflows
- **Kafka 7.4.0** — Streaming data
- **Spark 4.0.2** — Big Data processing
- **HBase 2.1** — NoSQL database
- **Hive 4.0.0** — SQL warehouse
- **PostgreSQL 15** — Relational DB
- **MLflow 2.8.1** — ML tracking
- **Prometheus 2.47.0** — Metrics
- **Grafana 10.2.0** — Dashboards
- **NiFi 1.25.0** — Data flow automation
- **ZooKeeper 7.4.0** — Distributed coordination

## 📝 Notes

- Tous les volumes sont nommés et persistants
- Les données survivent aux redémarrages (sauf `docker compose down -v`)
- Les logs sont stockés dans `./logs/airflow/`
- Les configurations sont dans `./config/`

## 🎓 Prochaines étapes

1. Ajouter vos DAGs dans `./dags/`
2. Configurer Grafana dashboards
3. Créer vos pipelines Kafka
4. Déployer vos modèles ML avec MLflow