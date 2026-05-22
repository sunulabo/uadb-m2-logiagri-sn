"""
streaming_logistics.py — Pipeline Spark Streaming Logi-Agri SN (v2+)
UADB | Master 2 Big Data & IA | 2025-2026
Améliorations v2+ :
  - Écriture HBase via foreachBatch (persistance temps réel)
  - Privacy layer SHA-256 + sel
  - 3 queries indépendants (transport, capteurs, stocks)
  - COALESCE systématique sur tous les nullables
  - Scoring ML + fallback règles expertes
  - Dead Letter Queue (DLQ) pour messages invalides
  - Logging structuré avec métriques par batch
"""

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sha2, concat, lit, from_json, to_json, struct,
    window, avg, max as spark_max, min as spark_min,
    when, current_timestamp, round as spark_round,
    coalesce, count, sum as spark_sum,
    expr, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, TimestampType
)

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
)
logger = logging.getLogger('LogiAgriStreaming')

# ── Configuration ──────────────────────────────────────────────────────────
SALT         = os.environ.get('LOGI_SECRET_SALT', 'logi_agri_sn_2025_uadb_secret')
BROKERS      = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
HBASE_HOST   = os.environ.get('HBASE_HOST', 'hbase')
MODEL_PATH   = os.environ.get('MODEL_PATH', 'hdfs:///logi_agri/models/perte_model_latest')
CHECKPOINT   = '/tmp/logi_checkpoints'


# ══════════════════════════════════════════════════════════════════════════
# SESSION SPARK
# ══════════════════════════════════════════════════════════════════════════
spark = (SparkSession.builder
    .appName('LogiAgri_SN_Streaming_v2')
    .config('spark.sql.shuffle.partitions', '4')
    .config('spark.streaming.stopGracefullyOnShutdown', 'true')
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
    .config('spark.sql.streaming.checkpointLocation', CHECKPOINT)
    .config('spark.executor.memory', '2g')
    .config('spark.driver.memory', '1g')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')
logger.info('✅ Session Spark démarrée')


# ══════════════════════════════════════════════════════════════════════════
# SCHÉMAS
# ══════════════════════════════════════════════════════════════════════════
TRANSPORT_SCHEMA = StructType([
    StructField('raw_entity_id',   StringType(),  True),
    StructField('voyage_id',       StringType(),  True),
    StructField('produit',         StringType(),  True),
    StructField('poids_kg',        FloatType(),   True),
    StructField('temp_moyenne_c',  FloatType(),   True),
    StructField('temp_max_tolere', FloatType(),   True),
    StructField('delai_prevu_h',   IntegerType(), True),
    StructField('date_depart',     StringType(),  True),
    StructField('zone_origine',    StringType(),  True),
    StructField('zone_dest',       StringType(),  True),
    StructField('distance_km',     FloatType(),   True),
    StructField('cout_fcfa_km',    FloatType(),   True),
    StructField('humidite_pct',    FloatType(),   True),
    StructField('pct_perte_reel',  FloatType(),   True),
    StructField('statut',          StringType(),  True),
])

CAPTEUR_SCHEMA = StructType([
    StructField('capteur_id',     StringType(),  True),
    StructField('voyage_id',      StringType(),  True),
    StructField('timestamp_utc',  StringType(),  True),
    StructField('temperature_c',  FloatType(),   True),
    StructField('latitude',       FloatType(),   True),
    StructField('longitude',      FloatType(),   True),
    StructField('batterie_pct',   FloatType(),   True),
    StructField('signal_qualite', IntegerType(), True),
    StructField('panne',          BooleanType(), True),
])

STOCK_SCHEMA = StructType([
    StructField('entrepot_id',    StringType(),  True),
    StructField('produit',        StringType(),  True),
    StructField('quantite_kg',    FloatType(),   True),
    StructField('temp_entrepot_c',FloatType(),   True),
    StructField('humidite_pct',   FloatType(),   True),
    StructField('capacite_max_kg',FloatType(),   True),
    StructField('date_entree',    StringType(),  True),
    StructField('zone',           StringType(),  True),
])


# ══════════════════════════════════════════════════════════════════════════
# TABLE RÉFÉRENCE COÛTS (broadcast statique)
# ══════════════════════════════════════════════════════════════════════════
COUT_REF = spark.createDataFrame([
    ('CASAMANCE',        'DAKAR',   490, 480.0),
    ('CASAMANCE',        'EXPORT',  520, 510.0),
    ('CASAMANCE',        'THIES',   460, 460.0),
    ('BASSIN_ARACHIDIER','DAKAR',   190, 310.0),
    ('BASSIN_ARACHIDIER','KAOLACK',  50, 260.0),
    ('BASSIN_ARACHIDIER','THIES',   150, 300.0),
    ('SINE_SALOUM',      'DAKAR',   280, 355.0),
    ('SINE_SALOUM',      'KAOLACK',  80, 270.0),
    ('NIAYES',           'DAKAR',    45, 290.0),
    ('NIAYES',           'THIES',    70, 280.0),
], ['zone_origine', 'zone_dest', 'dist_ref_km', 'cout_ref_fcfa_km'])


# ══════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════
def read_kafka(topic: str, schema: StructType) -> DataFrame:
    """Lecture Kafka avec parsing JSON et horodatage d'ingestion."""
    return (spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', BROKERS)
        .option('subscribe', topic)
        .option('startingOffsets', 'latest')
        .option('maxOffsetsPerTrigger', 1000)
        .option('failOnDataLoss', 'false')
        .load()
        .select(from_json(col('value').cast('string'), schema).alias('d'))
        .select('d.*')
        .withColumn('event_ts', current_timestamp())
    )


def write_to_hbase_batch(batch_df: DataFrame, batch_id: int, table: str) -> None:
    """
    Écrit un micro-batch dans HBase via foreachBatch.
    Architecture correcte : Stream → foreachBatch → HBase
    """
    try:
        rows = batch_df.collect()
        if not rows:
            return

        import happybase
        conn = happybase.Connection(HBASE_HOST, port=9090, timeout=10000)
        conn.open()
        hbase_table = conn.table(table.encode())

        with hbase_table.batch(transaction=True) as b:
            for row in rows:
                row_dict = row.asDict()
                voyage_id = row_dict.get('voyage_id', f'unknown_{batch_id}')
                row_key = f"{voyage_id}_{batch_id}_{id(row)}".encode()

                # Famille kpi
                kpi_data = {}
                for field in ['cout_total_fcfa', 'cout_par_tonne_fcfa',
                              'risque_perte_score', 'statut_alerte']:
                    val = row_dict.get(field)
                    if val is not None:
                        kpi_data[f'kpi:{field}'.encode()] = str(val).encode()

                # Famille meta
                meta_data = {}
                for field in ['voyage_id', 'produit', 'zone_origine', 'zone_dest',
                              'date_depart', 'statut']:
                    val = row_dict.get(field)
                    if val is not None:
                        meta_data[f'meta:{field}'.encode()] = str(val).encode()

                b.put(row_key, {**kpi_data, **meta_data})

        conn.close()
        logger.info(f'[HBase:{table}] Batch {batch_id} écrit | {len(rows)} lignes')

    except Exception as exc:
        logger.error(f'[HBase:{table}] Erreur batch {batch_id} : {exc}')
        # DLQ : écrire sur Kafka logi_dlq pour investigation
        try:
            error_df = batch_df.withColumn('error', lit(str(exc)))
            (error_df.select(to_json(struct('*')).alias('value'))
             .write.format('kafka')
             .option('kafka.bootstrap.servers', BROKERS)
             .option('topic', 'logi_dlq')
             .save())
        except Exception:
            pass


def log_batch_metrics(batch_df: DataFrame, batch_id: int, stream_name: str) -> None:
    """Log les métriques d'un micro-batch pour monitoring."""
    total = batch_df.count()
    if total == 0:
        return
    logger.info(f'[{stream_name}] Batch {batch_id} | {total} événements traités')


# ══════════════════════════════════════════════════════════════════════════
# CHARGEMENT MODÈLE ML
# ══════════════════════════════════════════════════════════════════════════
model = None
try:
    from pyspark.ml import PipelineModel
    model = PipelineModel.load(MODEL_PATH)
    logger.info(f'✅ Modèle ML chargé depuis : {MODEL_PATH}')
except Exception as exc:
    logger.warning(f'[ML] Modèle absent ou non chargeable ({exc})')
    logger.warning('[ML] Fallback sur scoring par règles expertes')


# ══════════════════════════════════════════════════════════════════════════
# QUERY 1 — PIPELINE TRANSPORT PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════
transport_df = read_kafka('logi_raw', TRANSPORT_SCHEMA)

# ── Privacy Layer : anonymisation PII ─────────────────────────────────────
secure_df = (transport_df
    .withColumn('entity_id_secure',
                sha2(concat(coalesce(col('raw_entity_id'), lit('UNKNOWN')),
                            lit(SALT)), 256))
    .drop('raw_entity_id')   # Suppression immédiate PII
)

# ── KPI 1 : Coût logistique ────────────────────────────────────────────────
kpi_cout = (secure_df
    .join(COUT_REF, on=['zone_origine', 'zone_dest'], how='left')
    .withColumn('cout_fcfa_km_eff',
                coalesce(col('cout_fcfa_km'),
                         col('cout_ref_fcfa_km'),
                         lit(400.0)))   # Fallback tarif moyen national
    .withColumn('distance_km_eff',
                coalesce(col('distance_km'),
                         col('dist_ref_km').cast(FloatType()),
                         lit(200.0)))
    .withColumn('cout_total_fcfa',
                spark_round(col('distance_km_eff') * col('cout_fcfa_km_eff'), 0))
    .withColumn('poids_kg_safe',
                coalesce(col('poids_kg'), lit(1000.0)))  # Évite division par zéro
    .withColumn('cout_par_tonne_fcfa',
                spark_round(col('cout_total_fcfa') / (col('poids_kg_safe') / 1000.0), 0))
)

# ── KPI 2 : Scoring risque de perte ───────────────────────────────────────
enriched = (kpi_cout
    .withColumn('depassement_temp',
                coalesce(
                    when(col('temp_moyenne_c') > col('temp_max_tolere'),
                         col('temp_moyenne_c') - col('temp_max_tolere'))
                    .otherwise(lit(0.0)),
                    lit(0.0)
                ))
    .withColumn('risque_perte_score',
                spark_round(
                    coalesce(
                        col('pct_perte_reel'),
                        (col('depassement_temp') / lit(10.0)) +
                        (coalesce(col('delai_prevu_h'), lit(24)).cast(FloatType()) / lit(100.0))
                    ),
                    3
                ))
    .withColumn('statut_alerte',
                when(col('risque_perte_score') > 0.4, lit('CRITIQUE'))
                .when(col('risque_perte_score') > 0.2, lit('ATTENTION'))
                .otherwise(lit('NORMAL')))
    .withColumn('ingestion_ts', current_timestamp())
)

# ── Application modèle ML si disponible ───────────────────────────────────
if model is not None:
    try:
        enriched = model.transform(enriched)
        enriched = enriched.withColumn(
            'risque_perte_score',
            coalesce(col('prediction').cast(FloatType()), col('risque_perte_score'))
        ).drop('prediction', 'features', 'features_raw', 'produit_idx')
        logger.info('[ML] Scoring ML actif')
    except Exception as exc:
        logger.warning(f'[ML] Erreur transform : {exc} — maintien scoring règles')

# ── Écriture 1a : Topic Kafka logi_alertes ─────────────────────────────────
q1a = (enriched
    .select(to_json(struct(
        'voyage_id', 'produit', 'zone_origine', 'zone_dest',
        'risque_perte_score', 'statut_alerte', 'cout_par_tonne_fcfa',
        'depassement_temp', 'temp_moyenne_c', 'ingestion_ts'
    )).alias('value'))
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'logi_alertes')
    .option('checkpointLocation', f'{CHECKPOINT}/transport_kafka')
    .outputMode('append')
    .start()
)
logger.info('✅ Query 1a démarrée : Transport → Kafka logi_alertes')

# ── Écriture 1b : HBase logi:transports ───────────────────────────────────
q1b = (enriched
    .writeStream
    .foreachBatch(lambda df, bid: write_to_hbase_batch(df, bid, 'logi:transports'))
    .option('checkpointLocation', f'{CHECKPOINT}/transport_hbase')
    .outputMode('append')
    .start()
)
logger.info('✅ Query 1b démarrée : Transport → HBase logi:transports')

# ── Écriture alertes critiques : HBase logi:alertes ───────────────────────
critiques_df = enriched.filter(col('statut_alerte') == 'CRITIQUE')
q1c = (critiques_df
    .writeStream
    .foreachBatch(lambda df, bid: write_to_hbase_batch(df, bid, 'logi:alertes'))
    .option('checkpointLocation', f'{CHECKPOINT}/alertes_hbase')
    .outputMode('append')
    .start()
)
logger.info('✅ Query 1c démarrée : Alertes CRITIQUES → HBase logi:alertes')


# ══════════════════════════════════════════════════════════════════════════
# QUERY 2 — AGRÉGATION CAPTEURS IoT (stream indépendant)
# ══════════════════════════════════════════════════════════════════════════
capteur_df = read_kafka('logi_capteurs', CAPTEUR_SCHEMA)

capteur_valides = (capteur_df
    .filter(col('panne') == False)
    .filter(col('temperature_c').isNotNull())
    .filter(col('latitude').between(12.0, 15.5))
    .filter(col('longitude').between(-17.5, -11.5))
    .withColumn('ts_parsed',
                to_timestamp(col('timestamp_utc'), 'yyyy-MM-dd\'T\'HH:mm:ss'))
)

temp_agg = (capteur_valides
    .withWatermark('event_ts', '30 minutes')
    .groupBy(window('event_ts', '1 hour', '15 minutes'), 'voyage_id')
    .agg(
        spark_max('temperature_c').alias('temp_max_reelle'),
        avg('temperature_c').alias('temp_moy_reelle'),
        spark_min('temperature_c').alias('temp_min_reelle'),
        count('capteur_id').alias('nb_releves'),
        avg('batterie_pct').alias('batterie_moy'),
    )
    .select(to_json(struct('*')).alias('value'))
)

q2 = (temp_agg
    .writeStream
    .format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'logi_temp_agg')
    .option('checkpointLocation', f'{CHECKPOINT}/capteurs')
    .outputMode('update')
    .start()
)
logger.info('✅ Query 2 démarrée : Capteurs IoT → Kafka logi_temp_agg')


# ══════════════════════════════════════════════════════════════════════════
# QUERY 3 — STOCKS ERP (stream indépendant)
# ══════════════════════════════════════════════════════════════════════════
stock_df = read_kafka('logi_stocks_erp', STOCK_SCHEMA)

stock_enriched = (stock_df
    .withColumn('taux_remplissage_pct',
                spark_round(
                    (coalesce(col('quantite_kg'), lit(0.0)) /
                     coalesce(col('capacite_max_kg'), lit(100000.0))) * 100.0,
                    1
                ))
    .withColumn('alerte_stock',
                when(col('taux_remplissage_pct') > 90, lit('SATURATION'))
                .when(col('taux_remplissage_pct') < 10, lit('RUPTURE_IMMINENTE'))
                .otherwise(lit('NORMAL')))
    .withColumn('ingestion_ts', current_timestamp())
)

q3 = (stock_enriched
    .writeStream
    .foreachBatch(lambda df, bid: write_to_hbase_batch(df, bid, 'logi:stocks'))
    .option('checkpointLocation', f'{CHECKPOINT}/stocks')
    .outputMode('append')
    .start()
)
logger.info('✅ Query 3 démarrée : Stocks ERP → HBase logi:stocks')


# ══════════════════════════════════════════════════════════════════════════
# MONITORING — Log métriques toutes les minutes
# ══════════════════════════════════════════════════════════════════════════
import threading

def log_query_status():
    """Thread de monitoring des queries Spark Streaming."""
    import time
    while True:
        time.sleep(60)
        for q_name, q in [('Transport', q1a), ('TempAgg', q2), ('Stocks', q3)]:
            try:
                prog = q.lastProgress
                if prog:
                    input_rows = prog.get('numInputRows', 0)
                    proc_rate = prog.get('processedRowsPerSecond', 0)
                    logger.info(
                        f'[{q_name}] inputRows={input_rows} | '
                        f'rate={proc_rate:.1f} rows/s'
                    )
            except Exception:
                pass

monitor_thread = threading.Thread(target=log_query_status, daemon=True)
monitor_thread.start()

logger.info('🚀 Toutes les queries Spark Streaming actives — en attente d\'événements...')
logger.info(f'   Kafka : {BROKERS}')
logger.info(f'   HBase : {HBASE_HOST}')
logger.info(f'   Modèle ML : {"ACTIF" if model else "FALLBACK règles expertes"}')

# Attendre la terminaison de la query principale
q1a.awaitTermination()
