#!/usr/bin/env python3
# streaming_logistics.py — Pipeline Spark Streaming Logi-Agri SN
# ✓ CORRECTIONS v2 :
# 1. Jointure stream-stream remplacée par 2 queries indépendantes
# 2. COALESCE sur tous les champs nullables
# 3. Division par zéro protégée

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit, from_json, to_json, struct,
    window, avg, max as spark_max, when, current_timestamp,
    round as spark_round, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Configuration
SALT = os.environ.get('LOGI_SECRET_SALT', 'logi_agri_sn_2025_uadb_secret')
BROKERS = 'localhost:9092'

# Initialiser Spark Session
spark = SparkSession.builder \
    .appName('LogiAgri_SN_Streaming') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config('spark.driver.memory', '2g') \
    .config('spark.executor.memory', '2g') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Schémas des données
transport_schema = StructType([
    StructField('raw_entity_id', StringType(), True),
    StructField('voyage_id', StringType(), True),
    StructField('produit', StringType(), True),
    StructField('poids_kg', FloatType(), True),
    StructField('temp_moyenne_c', FloatType(), True),
    StructField('temp_max_tolere', FloatType(), True),
    StructField('delai_prevu_h', IntegerType(), True),
    StructField('date_depart', StringType(), True),
    StructField('zone_origine', StringType(), True),
    StructField('zone_dest', StringType(), True),
    StructField('distance_km', FloatType(), True),
    StructField('cout_fcfa_km', FloatType(), True),
    StructField('pct_perte_reel', FloatType(), True),
])

capteur_schema = StructType([
    StructField('capteur_id', StringType(), True),
    StructField('voyage_id', StringType(), True),
    StructField('timestamp_utc', StringType(), True),
    StructField('temperature_c', FloatType(), True),
    StructField('latitude', FloatType(), True),
    StructField('longitude', FloatType(), True),
])

# Table de référence coûts (statique, pas un stream)
cout_ref_data = [
    ('CASAMANCE', 'DAKAR', 490, 450.0),
    ('CASAMANCE', 'EXPORT', 520, 480.0),
    ('BASSIN_ARACHIDIER', 'DAKAR', 190, 300.0),
    ('BASSIN_ARACHIDIER', 'KAOLACK', 50, 250.0),
    ('SINE_SALOUM', 'DAKAR', 280, 350.0),
    ('NIAYES', 'DAKAR', 45, 280.0),
]
cout_ref = spark.createDataFrame(
    cout_ref_data,
    ['zone_origine', 'zone_dest', 'dist_ref_km', 'cout_ref_fcfa_km']
)

def read_kafka(topic, schema):
    """Lit un topic Kafka et applique le schéma"""
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', BROKERS)
            .option('subscribe', topic)
            .option('startingOffsets', 'latest')
            .load()
            .select(from_json(col('value').cast('string'), schema).alias('d'))
            .select('d.*')
            .withColumn('event_ts', current_timestamp())
    )

print('[Spark] Lecture des streams Kafka...')
transport_df = read_kafka('logi_raw', transport_schema)
capteur_df = read_kafka('logi_capteurs', capteur_schema)

# ── Privacy Layer ────────────────────────────────────────────────────────
# Anonymiser raw_entity_id en SHA-256 avec sel
print('[Privacy] Application de la couche de confidentialité...')
secure_df = (transport_df
    .withColumn('entity_id_secure',
        sha2(concat(col('raw_entity_id'), lit(SALT)), 256))
    .drop('raw_entity_id')
)

# ── KPI 1 : Coût logistique ──────────────────────────────────────────────
print('[KPI] Calcul des coûts logistiques...')
kpi_cout = (secure_df
    .join(cout_ref, on=['zone_origine','zone_dest'], how='left')
    .withColumn('cout_fcfa_km_eff',
        coalesce(col('cout_fcfa_km'), col('cout_ref_fcfa_km'), lit(400.0)))
    .withColumn('cout_total_fcfa',
        spark_round(col('distance_km') * col('cout_fcfa_km_eff'), 0))
    .withColumn('poids_kg_safe',
        coalesce(col('poids_kg'), lit(1.0)))  # Protection division /0
    .withColumn('cout_par_tonne_fcfa',
        spark_round(col('cout_total_fcfa') / (col('poids_kg_safe')/1000.0), 0))
)

# ── KPI 2 : Scoring risque de perte ──────────────────────────────────────
print('[KPI] Calcul des risques de perte...')
enriched = (kpi_cout
    .withColumn('depassement_temp',
        when(col('temp_moyenne_c') > col('temp_max_tolere'),
            col('temp_moyenne_c') - col('temp_max_tolere')).otherwise(0.0))
    .withColumn('risque_perte_score',
        spark_round(
            coalesce(col('pct_perte_reel'),
                (col('depassement_temp')/10.0) + (col('delai_prevu_h')/100.0)),
            3))
    .withColumn('statut_alerte',
        when(col('risque_perte_score') > 0.4, lit('CRITIQUE'))
        .when(col('risque_perte_score') > 0.2, lit('ATTENTION'))
        .otherwise(lit('NORMAL')))
)

# ── Query 1 : Écrire les alertes ─────────────────────────────────────────
print('[Spark] Démarrage Query 1 : Écriture des alertes...')
q1 = (enriched
    .select(to_json(struct('*')).alias('value'))
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'logi_alertes')
    .option('checkpointLocation', '/tmp/logi_transport_ckpt')
    .outputMode('append')
    .start()
)

# ── Query 2 : Agrégation capteurs IoT ────────────────────────────────────
print('[Spark] Démarrage Query 2 : Agrégation capteurs...')
q2 = (capteur_df
    .withWatermark('event_ts', '30 minutes')
    .groupBy(window('event_ts', '1 hour'), 'voyage_id')
    .agg(
        spark_max('temperature_c').alias('temp_max_reelle'),
        avg('temperature_c').alias('temp_moy_reelle')
    )
    .select(to_json(struct('*')).alias('value'))
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'logi_temp_agg')
    .option('checkpointLocation', '/tmp/logi_capteurs_ckpt')
    .outputMode('update')
    .start()
)

print('[Spark] ✓ Pipeline démarré, les 2 queries tournent...')
print('[Spark] Streaming...')
q1.awaitTermination()