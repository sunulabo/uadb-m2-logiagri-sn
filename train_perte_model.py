# train_perte_model.py — Random Forest sur historique Hive
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import argparse, os

parser = argparse.ArgumentParser()
parser.add_argument('--output-path', default='models/perte_model_latest')
parser.add_argument('--window-days', type=int, default=90)
args = parser.parse_args()

spark = SparkSession.builder.appName('LogiAgri_Train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Simulation de données historiques pour l'entraînement
from pyspark.sql.functions import rand, randn
import pyspark.sql.functions as F

print('Génération des données historiques simulées...')
df = spark.range(1000).select(
    (F.rand() * 30 + 15).alias('temp_moyenne_c'),
    (F.rand() * 20 + 10).alias('temp_max_tolere'),
    (F.rand() * 100 + 1).cast('int').alias('delai_prevu_h'),
    (F.rand() * 500 + 50).alias('distance_km'),
    (F.rand() * 10000 + 500).alias('poids_kg'),
    (F.array(F.lit('ARACHIDE'), F.lit('MANGUE'), F.lit('RIZ'), F.lit('TOMATE'))
     .getItem((F.rand() * 4).cast('int'))).alias('produit'),
    (F.rand() * 0.5).alias('pct_perte_reel')
).na.drop()

train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f'Train={train.count()} | Test={test.count()}')

pipeline = Pipeline(stages=[
    StringIndexer(inputCol='produit', outputCol='produit_idx'),
    VectorAssembler(
        inputCols=['temp_moyenne_c', 'temp_max_tolere', 'delai_prevu_h',
                   'distance_km', 'poids_kg', 'produit_idx'],
        outputCol='features_raw'
    ),
    StandardScaler(inputCol='features_raw', outputCol='features',
                   withStd=True, withMean=True),
    RandomForestRegressor(featuresCol='features', labelCol='pct_perte_reel',
                          numTrees=100, maxDepth=8, seed=42),
])

print('Entraînement du modèle Random Forest...')
model = pipeline.fit(train)
preds = model.transform(test)

ev = RegressionEvaluator(labelCol='pct_perte_reel', predictionCol='prediction')
rmse = ev.setMetricName('rmse').evaluate(preds)
r2 = ev.setMetricName('r2').evaluate(preds)
print(f'RMSE={rmse:.4f}')
print(f'R²  ={r2:.4f}')

os.makedirs(args.output_path, exist_ok=True)
model.write().overwrite().save(args.output_path)
print(f'Modèle sauvegardé : {args.output_path}')
spark.stop()