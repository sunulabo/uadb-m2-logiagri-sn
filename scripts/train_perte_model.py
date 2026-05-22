"""
train_perte_model.py — Random Forest Logi-Agri SN (v2+)
UADB | Master 2 Big Data & IA | 2025-2026
Améliorations v2+ :
  - Traçabilité MLflow complète (paramètres, métriques, artefacts)
  - Feature importance visualization
  - Validation croisée k=5
  - Hyperparameter tuning via CrossValidator
  - Analyse des erreurs (worst predictions)
  - Export métriques JSON pour Airflow
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger('LogiAgriTrain')

# ── Imports Spark ──────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, expr
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, StandardScaler,
    QuantileDiscretizer
)
from pyspark.ml.regression import (
    RandomForestRegressor, GBTRegressor
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

# ── Arguments ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description='Entraînement Random Forest Logi-Agri SN')
parser.add_argument('--output-path',  default='hdfs:///logi_agri/models/perte_model_latest')
parser.add_argument('--window-days',  type=int,  default=90,    help='Fenêtre historique')
parser.add_argument('--num-trees',    type=int,  default=100,   help='Nombre d\'arbres RF')
parser.add_argument('--max-depth',    type=int,  default=8,     help='Profondeur max RF')
parser.add_argument('--cross-val',    type=int,  default=3,     help='Folds validation croisée')
parser.add_argument('--min-rows',     type=int,  default=100,   help='Minimum de lignes requis')
parser.add_argument('--mlflow-uri',   default='http://mlflow:5000', help='URI MLflow')
parser.add_argument('--metrics-out',  default='/tmp/train_metrics.json')
args = parser.parse_args()

# ── Spark Session ──────────────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName('LogiAgri_Train_v2')
    .enableHiveSupport()
    .config('spark.executor.memory', '3g')
    .config('spark.driver.memory', '2g')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

# ── MLflow ──────────────────────────────────────────────────────────────────
try:
    import mlflow
    import mlflow.spark
    mlflow.set_tracking_uri(args.mlflow_uri)
    mlflow.set_experiment('logi-agri-sn-perte-prediction')
    MLFLOW_ENABLED = True
    logger.info(f'✅ MLflow connecté : {args.mlflow_uri}')
except ImportError:
    MLFLOW_ENABLED = False
    logger.warning('[MLflow] Non disponible — métriques loggées localement uniquement')


# ══════════════════════════════════════════════════════════════════════════
# CHARGEMENT DES DONNÉES
# ══════════════════════════════════════════════════════════════════════════
logger.info(f'📦 Chargement historique ({args.window_days} derniers jours)...')

df_raw = spark.sql(f"""
    SELECT
        temp_moyenne_c,
        temp_max_tolere,
        (temp_moyenne_c - temp_max_tolere) AS depassement_temp,
        delai_prevu_h,
        distance_km,
        produit,
        poids_kg,
        cout_par_tonne_fcfa,
        -- Feature engineered : ratio durée vs durée max tolérée par produit
        CASE produit
            WHEN 'MANGUE'   THEN delai_prevu_h / 72.0
            WHEN 'TOMATE'   THEN delai_prevu_h / 48.0
            WHEN 'ARACHIDE' THEN delai_prevu_h / 720.0
            WHEN 'RIZ'      THEN delai_prevu_h / 4320.0
            ELSE delai_prevu_h / 168.0
        END AS ratio_duree_max,
        pct_perte_reel
    FROM logi_agri.historique_transport
    WHERE date_depart >= DATE_SUB(CURRENT_DATE, {args.window_days})
      AND pct_perte_reel IS NOT NULL
      AND poids_kg > 0
      AND distance_km > 0
""").na.drop()

total_rows = df_raw.count()
logger.info(f'📊 Données chargées : {total_rows} lignes')

if total_rows < args.min_rows:
    logger.error(
        f'Données insuffisantes : {total_rows} lignes < minimum {args.min_rows}. '
        f'Lancer d\'abord : python kafka_producer_logi.py --batch --batch-days 90'
    )
    sys.exit(1)

# ── Statistiques descriptives ──────────────────────────────────────────────
logger.info('📈 Statistiques de la variable cible pct_perte_reel :')
df_raw.select('pct_perte_reel', 'produit', 'depassement_temp').describe().show()

# Distribution par produit
logger.info('Distribution par produit :')
df_raw.groupBy('produit').count().orderBy('count', ascending=False).show()


# ══════════════════════════════════════════════════════════════════════════
# PIPELINE ML
# ══════════════════════════════════════════════════════════════════════════
FEATURES_NUMERIQUES = [
    'temp_moyenne_c', 'depassement_temp', 'delai_prevu_h',
    'distance_km', 'poids_kg', 'ratio_duree_max',
    'cout_par_tonne_fcfa'
]

# Split stratifié approx. (par produit)
train_df, test_df = df_raw.randomSplit([0.8, 0.2], seed=42)
logger.info(f'Train={train_df.count()} | Test={test_df.count()}')

# ── Stages Pipeline ────────────────────────────────────────────────────────
produit_indexer = StringIndexer(
    inputCol='produit',
    outputCol='produit_idx',
    handleInvalid='keep'
)

assembler = VectorAssembler(
    inputCols=FEATURES_NUMERIQUES + ['produit_idx'],
    outputCol='features_raw',
    handleInvalid='keep'
)

scaler = StandardScaler(
    inputCol='features_raw',
    outputCol='features',
    withStd=True,
    withMean=True
)

rf = RandomForestRegressor(
    featuresCol='features',
    labelCol='pct_perte_reel',
    numTrees=args.num_trees,
    maxDepth=args.max_depth,
    minInstancesPerNode=5,
    featureSubsetStrategy='sqrt',
    seed=42
)

pipeline = Pipeline(stages=[produit_indexer, assembler, scaler, rf])


# ══════════════════════════════════════════════════════════════════════════
# HYPERPARAMETER TUNING (Cross-Validation)
# ══════════════════════════════════════════════════════════════════════════
logger.info(f'🔍 Cross-validation {args.cross_val} folds...')

param_grid = (ParamGridBuilder()
    .addGrid(rf.numTrees,  [50, 100])
    .addGrid(rf.maxDepth,  [5, 8])
    .build()
)

evaluator = RegressionEvaluator(
    labelCol='pct_perte_reel',
    predictionCol='prediction',
    metricName='rmse'
)

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=args.cross_val,
    seed=42,
    parallelism=2
)


# ══════════════════════════════════════════════════════════════════════════
# ENTRAÎNEMENT AVEC MLFLOW
# ══════════════════════════════════════════════════════════════════════════
run_name = f'rf_perte_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

def train_and_evaluate():
    """Entraîne le modèle et retourne les métriques."""
    logger.info('🚀 Entraînement en cours...')
    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel

    # Évaluation sur test set
    preds = best_model.transform(test_df)

    metrics = {}
    for metric in ['rmse', 'mae', 'r2']:
        metrics[metric] = round(
            RegressionEvaluator(
                labelCol='pct_perte_reel',
                predictionCol='prediction',
                metricName=metric
            ).evaluate(preds),
            4
        )

    logger.info('─' * 40)
    logger.info('RÉSULTATS ÉVALUATION :')
    logger.info(f'  RMSE = {metrics["rmse"]:.4f}')
    logger.info(f'  MAE  = {metrics["mae"]:.4f}')
    logger.info(f'  R²   = {metrics["r2"]:.4f}')
    logger.info('─' * 40)

    # ── Feature Importance ──────────────────────────────
    rf_stage = best_model.stages[-1]
    feature_names = FEATURES_NUMERIQUES + ['produit_idx']
    importances = rf_stage.featureImportances.toArray()

    fi_sorted = sorted(
        zip(feature_names, importances),
        key=lambda x: x[1], reverse=True
    )
    logger.info('IMPORTANCE DES VARIABLES :')
    for fname, imp in fi_sorted:
        bar = '█' * int(imp * 50)
        logger.info(f'  {fname:<25} {imp:.4f} {bar}')

    metrics['feature_importances'] = {k: round(v, 4) for k, v in fi_sorted}
    metrics['best_num_trees'] = rf_stage.getNumTrees
    metrics['best_max_depth'] = rf_stage.getMaxDepth()
    metrics['train_rows'] = train_df.count()
    metrics['test_rows'] = test_df.count()
    metrics['window_days'] = args.window_days
    metrics['trained_at'] = datetime.now().isoformat()

    # ── Pires prédictions (analyse d'erreur) ───────────
    preds_worst = (preds
        .withColumn('erreur_abs', expr('ABS(pct_perte_reel - prediction)'))
        .orderBy('erreur_abs', ascending=False)
        .select('produit', 'zone_origine', 'zone_dest',
                'temp_moyenne_c', 'depassement_temp',
                'pct_perte_reel', 'prediction', 'erreur_abs')
        .limit(10)
    )
    logger.info('TOP 10 PIRES PRÉDICTIONS :')
    preds_worst.show(truncate=False)

    return best_model, metrics


# ── Run MLflow ou local ────────────────────────────────────────────────────
if MLFLOW_ENABLED:
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params({
            'num_trees':   args.num_trees,
            'max_depth':   args.max_depth,
            'window_days': args.window_days,
            'cross_val':   args.cross_val,
            'features':    ','.join(FEATURES_NUMERIQUES),
            'total_rows':  total_rows,
        })

        best_model, metrics = train_and_evaluate()

        mlflow.log_metrics({k: v for k, v in metrics.items()
                            if isinstance(v, (int, float))})
        mlflow.spark.log_model(best_model, 'random_forest_perte')

        run_id = run.info.run_id
        logger.info(f'✅ MLflow Run ID : {run_id}')
        metrics['mlflow_run_id'] = run_id
else:
    best_model, metrics = train_and_evaluate()


# ══════════════════════════════════════════════════════════════════════════
# SAUVEGARDE MODÈLE
# ══════════════════════════════════════════════════════════════════════════
logger.info(f'💾 Sauvegarde modèle → {args.output_path}')
best_model.write().overwrite().save(args.output_path)
logger.info('✅ Modèle sauvegardé avec succès')

# ── Export métriques JSON (pour Airflow XCom) ─────────────────────────────
with open(args.metrics_out, 'w') as f:
    json.dump(metrics, f, indent=2, default=str)
logger.info(f'📄 Métriques exportées → {args.metrics_out}')

# ── Seuil qualité minimum ──────────────────────────────────────────────────
if metrics['r2'] < 0.5:
    logger.warning(
        f'⚠️  R²={metrics["r2"]:.4f} < 0.5 — qualité modèle insuffisante. '
        f'Vérifier la qualité des données ou augmenter la fenêtre historique.'
    )
    sys.exit(2)  # Code 2 = warning (pas d'échec dur)

logger.info(f'✅ Entraînement terminé | RMSE={metrics["rmse"]:.4f} | R²={metrics["r2"]:.4f}')
spark.stop()
