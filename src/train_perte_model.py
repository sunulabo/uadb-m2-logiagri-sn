#!/usr/bin/env python3
# train_perte_model.py — Entraînement Random Forest sur l'historique Hive

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('LogiAgri_Train')

# Paramètres
parser = argparse.ArgumentParser(description='Entraîner le modèle Random Forest')
parser.add_argument('--output-path', default='models/perte_model_latest',
                   help='Chemin de sortie du modèle')
parser.add_argument('--window-days', type=int, default=90,
                   help='Nombre de jours historiques à utiliser')
parser.add_argument('--num-trees', type=int, default=100,
                   help='Nombre d\'arbres dans la forêt')
args = parser.parse_args()

# Initialiser Spark
logger.info('Initialisation Spark...')
spark = SparkSession.builder \
    .appName('LogiAgri_Train') \
    .config('spark.driver.memory', '2g') \
    .config('spark.executor.memory', '2g') \
    .getOrCreate()

# Pour ce TP, on va générer des données simulées car Hive n'est pas encore rempli
# En production, on ferait :
# df = spark.sql(f"""
#     SELECT temp_moyenne_c, temp_max_tolere,
#            (temp_moyenne_c - temp_max_tolere) AS depassement_temp,
#            delai_prevu_h, distance_km, produit, poids_kg,
#            pct_perte_reel
#     FROM logi_agri.historique_transport
#     WHERE date_depart >= DATE_SUB(CURRENT_DATE, {args.window_days})
#     AND pct_perte_reel IS NOT NULL
# """)

# Pour le TP : génération de données synthétiques
import random
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType

logger.info('Génération de données synthétiques pour l\'entraînement...')
data = []
for i in range(1000):
    produit = random.choice(['ARACHIDE', 'MANGUE', 'RIZ', 'TOMATE'])
    temp = random.uniform(15, 45)
    temp_max = random.choice([10, 12, 30, 35])
    delai = random.randint(12, 200)
    dist = random.randint(45, 520)
    poids = random.randint(500, 15000)
    
    # Perte réaliste basée sur les paramètres
    depass = max(0, temp - temp_max) / 20.0
    perte = 0.02 + (depass * 0.3) + (delai / 1000.0 * 0.2) + random.uniform(-0.02, 0.02)
    perte = min(1.0, max(0.0, perte))
    
    data.append({
        'temp_moyenne_c': float(temp),
        'temp_max_tolere': float(temp_max),
        'depassement_temp': float(max(0, temp - temp_max)),
        'delai_prevu_h': int(delai),
        'distance_km': float(dist),
        'produit': produit,
        'poids_kg': float(poids),
        'pct_perte_reel': float(perte)
    })

schema = StructType([
    StructField('temp_moyenne_c', DoubleType()),
    StructField('temp_max_tolere', DoubleType()),
    StructField('depassement_temp', DoubleType()),
    StructField('delai_prevu_h', IntegerType()),
    StructField('distance_km', DoubleType()),
    StructField('produit', StringType()),
    StructField('poids_kg', DoubleType()),
    StructField('pct_perte_reel', DoubleType()),
])

df = spark.createDataFrame(data, schema)
logger.info(f'✓ {len(data)} exemples générés')

# Splitwaldtest
train, test = df.randomSplit([0.8, 0.2], seed=42)
logger.info(f'Train: {train.count()} | Test: {test.count()}')

# Pipeline ML
logger.info('Construction du pipeline ML...')
pipeline = Pipeline(stages=[
    StringIndexer(inputCol='produit', outputCol='produit_idx'),
    VectorAssembler(
        inputCols=['temp_moyenne_c', 'depassement_temp', 'delai_prevu_h',
                   'distance_km', 'poids_kg', 'produit_idx'],
        outputCol='features_raw'
    ),
    StandardScaler(
        inputCol='features_raw',
        outputCol='features',
        withStd=True,
        withMean=True
    ),
    RandomForestRegressor(
        featuresCol='features',
        labelCol='pct_perte_reel',
        numTrees=args.num_trees,
        maxDepth=8,
        seed=42
    ),
])

# Entraînement
logger.info('Entraînement du modèle Random Forest...')
model = pipeline.fit(train)

# Évaluation
logger.info('Évaluation sur le test set...')
preds = model.transform(test)

ev = RegressionEvaluator(labelCol='pct_perte_reel', predictionCol='prediction')
rmse = ev.setMetricName('rmse').evaluate(preds)
r2 = ev.setMetricName('r2').evaluate(preds)
mae = ev.setMetricName('mae').evaluate(preds)

logger.info(f'Résultats :')
logger.info(f'  RMSE = {rmse:.4f}')
logger.info(f'  R²   = {r2:.4f}')
logger.info(f'  MAE  = {mae:.4f}')

# Sauvegarde
logger.info(f'Sauvegarde du modèle vers {args.output_path}...')
model.write().overwrite().save(args.output_path)
logger.info(f'✓ Modèle sauvegardé')

spark.stop()
logger.info('✓ Entraînement terminé')