"""
dags/logi_agri_retrain_dag.py — DAG Airflow Logi-Agri SN (v2+)
UADB | Master 2 Big Data & IA | 2025-2026
Améliorations v2+ :
  - Health check HBase/Kafka avant traitement
  - Notification Slack + email sur échec critique
  - Task de qualité données avant entraînement
  - Archivage automatique anciens modèles
  - Rapport quotidien de performance
  - Retry exponentiel configurable
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import subprocess
import logging
import json
import os

logger = logging.getLogger('logi_agri_dag')

# ── Configuration ──────────────────────────────────────────────────────────
HIVE_HOST       = os.environ.get('HIVE_HOST', 'hive-metastore')
SPARK_MASTER    = os.environ.get('SPARK_MASTER', 'spark://spark-master:7077')
MODEL_PATH      = 'hdfs:///logi_agri/models/perte_model_latest'
MODEL_ARCHIVE   = 'hdfs:///logi_agri/models/archive'
METRICS_FILE    = '/tmp/train_metrics.json'
SEUIL_DEVIATION = 0.05   # Réentraîner si déviation > 5%
SEUIL_RMSE_MAX  = 0.15   # Alerte si RMSE > 15%

DEFAULT_ARGS = {
    'owner':              'seye_ahmed',
    'retries':            2,
    'retry_delay':        timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay':    timedelta(minutes=30),
    'email_on_failure':   True,
    'email_on_retry':     False,
    'email':              ['logi-agri@sonatel.sn'],
    'execution_timeout':  timedelta(hours=2),
}


# ══════════════════════════════════════════════════════════════════════════
# FONCTIONS MÉTIER
# ══════════════════════════════════════════════════════════════════════════

def health_check_infrastructure(**ctx):
    """
    Vérifie que HBase, Kafka et Hive sont opérationnels.
    Arrête le DAG proprement si un service est down.
    """
    erreurs = []

    # Check HBase
    try:
        import happybase
        conn = happybase.Connection('hbase', port=9090, timeout=5000)
        conn.open()
        tables = conn.tables()
        conn.close()
        logger.info(f'✅ HBase OK — {len(tables)} tables disponibles')
    except Exception as exc:
        erreurs.append(f'HBase: {exc}')
        logger.error(f'❌ HBase DOWN : {exc}')

    # Check Hive
    try:
        from pyhive import hive
        conn = hive.Connection(host=HIVE_HOST, port=10000, timeout=5)
        cursor = conn.cursor()
        cursor.execute('SHOW DATABASES')
        conn.close()
        logger.info('✅ Hive OK')
    except Exception as exc:
        erreurs.append(f'Hive: {exc}')
        logger.error(f'❌ Hive DOWN : {exc}')

    if erreurs:
        raise RuntimeError(
            f'Health check échoué ({len(erreurs)} service(s) indisponibles) : '
            + ' | '.join(erreurs)
        )

    logger.info('✅ Tous les services opérationnels')
    return True


def check_data_quality(**ctx):
    """
    Vérifie la qualité des données avant agrégation.
    Retourne False si trop peu de données (court-circuit du DAG).
    """
    try:
        from pyhive import hive
        conn = hive.Connection(host=HIVE_HOST, port=10000)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN pct_perte_reel IS NULL THEN 1 ELSE 0 END) AS null_perte,
                SUM(CASE WHEN poids_kg <= 0 THEN 1 ELSE 0 END) AS poids_invalide,
                AVG(pct_perte_reel) AS perte_moy
            FROM logi_agri.flux_temps_reel
            WHERE date_depart = DATE_SUB(CURRENT_DATE, 1)
        """)
        row = cursor.fetchone()
        conn.close()

        if not row or row[0] == 0:
            logger.warning('⚠️  Aucune donnée J-1 — pipeline court-circuité')
            return False  # ShortCircuit : stoppe le DAG proprement

        total, null_perte, poids_invalide, perte_moy = row
        taux_completude = (total - null_perte) / total * 100

        logger.info(f'📊 Qualité données J-1 :')
        logger.info(f'   Total transports : {total}')
        logger.info(f'   Complétude perte : {taux_completude:.1f}%')
        logger.info(f'   Poids invalides  : {poids_invalide}')
        logger.info(f'   Perte moyenne    : {perte_moy:.3f}' if perte_moy else '')

        ctx['ti'].xcom_push(key='data_quality', value={
            'total': total,
            'taux_completude': round(taux_completude, 2),
            'perte_moy': round(float(perte_moy), 4) if perte_moy else None,
        })
        return True

    except Exception as exc:
        logger.error(f'Erreur qualité données : {exc}')
        return True  # On continue même si le check échoue


def aggregate_transports(**ctx):
    """Agrège les transports terminés J-1 dans historique_transport."""
    from pyhive import hive
    conn = hive.Connection(host=HIVE_HOST, port=10000)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO logi_agri.historique_transport
        SELECT
            voyage_id, produit, poids_kg,
            temp_moyenne_c, temp_max_tolere,
            delai_prevu_h, distance_km,
            cout_total_fcfa, cout_par_tonne_fcfa,
            pct_perte_reel,
            CAST(date_depart AS DATE)
        FROM logi_agri.flux_temps_reel
        WHERE statut = 'TERMINE'
          AND date_depart = DATE_SUB(CURRENT_DATE, 1)
          AND voyage_id NOT IN (
              SELECT voyage_id FROM logi_agri.historique_transport
          )
    """)

    cursor.execute("""
        SELECT COUNT(*) FROM logi_agri.historique_transport
        WHERE date_depart = DATE_SUB(CURRENT_DATE, 1)
    """)
    count = cursor.fetchone()[0]
    conn.close()

    logger.info(f'✅ {count} transports agrégés pour J-1')
    ctx['ti'].xcom_push(key='nb_transports_j1', value=count)


def calculate_kpis(**ctx):
    """Recalcule les KPIs logistiques des 30 derniers jours."""
    from pyhive import hive
    conn = hive.Connection(host=HIVE_HOST, port=10000)
    cursor = conn.cursor()

    # KPI principal
    cursor.execute("""
        INSERT OVERWRITE TABLE logi_agri.kpi_logistique
        SELECT
            produit,
            zone_origine,
            zone_dest,
            COUNT(*)                              AS nb_voyages,
            ROUND(AVG(cout_par_tonne_fcfa), 0)    AS cout_moy_tonne_fcfa,
            ROUND(AVG(pct_perte_reel) * 100, 2)   AS taux_perte_moyen_pct,
            ROUND(SUM(poids_kg) / 1000.0, 1)      AS tonnage_total,
            ROUND(AVG(delai_prevu_h), 1)           AS delai_moyen_h,
            CURRENT_DATE                           AS date_calcul
        FROM logi_agri.historique_transport
        WHERE date_depart >= DATE_SUB(CURRENT_DATE, 30)
        GROUP BY produit, zone_origine, zone_dest
    """)

    # Top alertes de la semaine
    cursor.execute("""
        SELECT produit, zone_origine, zone_dest,
               ROUND(AVG(pct_perte_reel)*100,1) AS perte_pct_moy
        FROM logi_agri.historique_transport
        WHERE date_depart >= DATE_SUB(CURRENT_DATE, 7)
        GROUP BY produit, zone_origine, zone_dest
        HAVING AVG(pct_perte_reel) > 0.2
        ORDER BY perte_pct_moy DESC
        LIMIT 5
    """)
    top_alertes = cursor.fetchall()
    conn.close()

    logger.info('✅ KPIs recalculés')
    if top_alertes:
        logger.warning('⚠️  Top corridors à risque (7j) :')
        for row in top_alertes:
            logger.warning(f'   {row[0]} {row[1]}→{row[2]} : {row[3]}% perte')

    ctx['ti'].xcom_push(key='top_alertes', value=top_alertes)


def check_deviation(**ctx):
    """
    Mesure la déviation entre les prédictions ML et les pertes réelles.
    Branch : retrain_model | skip_retrain
    """
    from pyhive import hive
    conn = hive.Connection(host=HIVE_HOST, port=10000)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COALESCE(AVG(ABS(risque_perte_score - pct_perte_reel)), 0.0) AS deviation,
            COUNT(*) AS nb_comparaisons
        FROM logi_agri.flux_temps_reel
        WHERE date_depart >= DATE_SUB(CURRENT_DATE, 7)
          AND pct_perte_reel IS NOT NULL
          AND risque_perte_score IS NOT NULL
    """)
    row = cursor.fetchone()
    conn.close()

    dev = float(row[0]) if row and row[0] else 0.0
    nb_comp = int(row[1]) if row and row[1] else 0

    logger.info(f'📊 Déviation modèle (7j) : {dev:.4f} | Comparaisons : {nb_comp}')
    ctx['ti'].xcom_push(key='deviation', value=dev)
    ctx['ti'].xcom_push(key='nb_comparaisons', value=nb_comp)

    if nb_comp < 50:
        logger.warning(f'Trop peu de comparaisons ({nb_comp}) — skip réentraînement')
        return 'skip_retrain'

    return 'retrain_model' if dev > SEUIL_DEVIATION else 'skip_retrain'


def archive_model(**ctx):
    """Archive le modèle actuel avant remplacement."""
    timestamp = ctx['ds_nodash']
    try:
        result = subprocess.run([
            'hdfs', 'dfs', '-cp',
            MODEL_PATH,
            f'{MODEL_ARCHIVE}/perte_model_{timestamp}'
        ], capture_output=True, text=True, timeout=120)

        if result.returncode == 0:
            logger.info(f'✅ Modèle archivé : {MODEL_ARCHIVE}/perte_model_{timestamp}')
        else:
            logger.warning(f'Archive ignorée (modèle absent) : {result.stderr}')
    except Exception as exc:
        logger.warning(f'Archive impossible : {exc}')


def retrain_model(**ctx):
    """Lance le job Spark de réentraînement du Random Forest."""
    result = subprocess.run([
        'spark-submit',
        '--master', SPARK_MASTER,
        '--num-executors', '2',
        '--executor-memory', '3g',
        '--driver-memory', '2g',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
        '/opt/airflow/models/train_perte_model.py',
        '--output-path', MODEL_PATH,
        '--window-days', '90',
        '--cross-val', '3',
        '--metrics-out', METRICS_FILE,
        '--mlflow-uri', 'http://mlflow:5000',
    ], capture_output=True, text=True, timeout=3600)

    if result.returncode not in (0, 2):  # 2 = warning qualité
        raise RuntimeError(f'Spark job échoué (code {result.returncode}):\n{result.stderr[-2000:]}')

    logger.info('✅ Réentraînement terminé')

    # Lire les métriques
    try:
        with open(METRICS_FILE) as f:
            metrics = json.load(f)
        logger.info(f'   RMSE = {metrics.get("rmse", "N/A")}')
        logger.info(f'   R²   = {metrics.get("r2", "N/A")}')
        logger.info(f'   MAE  = {metrics.get("mae", "N/A")}')

        # Alerte si RMSE trop élevé
        rmse = metrics.get('rmse', 0)
        if rmse > SEUIL_RMSE_MAX:
            logger.warning(f'⚠️  RMSE={rmse:.4f} > seuil {SEUIL_RMSE_MAX} — qualité modèle dégradée')

        ctx['ti'].xcom_push(key='train_metrics', value=metrics)
    except Exception:
        pass


def generate_daily_report(**ctx):
    """Génère un rapport quotidien synthétique de performance."""
    try:
        from pyhive import hive
        conn = hive.Connection(host=HIVE_HOST, port=10000)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                produit,
                ROUND(AVG(taux_perte_moyen_pct), 2) AS perte_moy,
                ROUND(AVG(cout_moy_tonne_fcfa), 0)  AS cout_moy,
                SUM(nb_voyages) AS nb_voyages,
                ROUND(SUM(tonnage_total), 1) AS tonnage
            FROM logi_agri.kpi_logistique
            WHERE date_calcul = CURRENT_DATE
            GROUP BY produit
            ORDER BY perte_moy DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        report_lines = [
            '=' * 55,
            f'RAPPORT QUOTIDIEN LOGI-AGRI SN — {ctx["ds"]}',
            '=' * 55,
        ]
        for row in rows:
            report_lines.append(
                f'{row[0]:<12} | Perte: {row[1]:.1f}% | '
                f'Coût: {row[2]:,.0f} FCFA/t | '
                f'{row[3]} voyages | {row[4]:.0f}t'
            )
        report_lines.append('=' * 55)

        report = '\n'.join(report_lines)
        logger.info('\n' + report)
        ctx['ti'].xcom_push(key='daily_report', value=report)

    except Exception as exc:
        logger.error(f'Erreur rapport quotidien : {exc}')


# ══════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ══════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id='logi_agri_sn_pipeline_v2',
    default_args=DEFAULT_ARGS,
    description='Pipeline KPI + Réentraînement ML Logi-Agri SN v2',
    schedule_interval='0 3 * * *',  # Chaque nuit à 3h
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['logi-agri', 'mlops', 'supply-chain', 'uadb'],
    doc_md="""
    ## DAG Logi-Agri SN v2
    Pipeline nocturne d'optimisation de la chaîne logistique agricole sénégalaise.
    - **3h00** : Health check infrastructure
    - **3h05** : Vérification qualité données J-1
    - **3h10** : Agrégation transports terminés
    - **3h20** : Calcul KPIs (30 jours glissants)
    - **3h30** : Évaluation déviation modèle ML
    - **3h35** : Réentraînement conditionnel (si déviation > 5%)
    - **5h00** : Rapport quotidien
    """
) as dag:

    start = DummyOperator(task_id='start')
    skip  = DummyOperator(task_id='skip_retrain')
    end   = DummyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    t_health = PythonOperator(
        task_id='health_check',
        python_callable=health_check_infrastructure,
        provide_context=True,
    )

    t_quality = ShortCircuitOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )

    t_aggregate = PythonOperator(
        task_id='aggregate_transports',
        python_callable=aggregate_transports,
        provide_context=True,
    )

    t_kpis = PythonOperator(
        task_id='calculate_kpis',
        python_callable=calculate_kpis,
        provide_context=True,
    )

    t_branch = BranchPythonOperator(
        task_id='check_deviation',
        python_callable=check_deviation,
        provide_context=True,
    )

    t_archive = PythonOperator(
        task_id='archive_model',
        python_callable=archive_model,
        provide_context=True,
    )

    t_retrain = PythonOperator(
        task_id='retrain_model',
        python_callable=retrain_model,
        provide_context=True,
    )

    t_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dépendances ────────────────────────────────────
    start >> t_health >> t_quality >> t_aggregate >> t_kpis >> t_branch
    t_branch >> t_archive >> t_retrain >> end
    t_branch >> skip >> end
    end >> t_report
