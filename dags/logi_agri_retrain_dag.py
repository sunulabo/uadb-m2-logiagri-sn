#!/usr/bin/env python3
# dags/logi_agri_retrain_dag.py — DAG Airflow pour Logi-Agri SN

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import subprocess
import os

logger = logging.getLogger('logi_agri_dag')

# Configuration Airflow
default_args = {
    'owner': 'logi_agri_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def dummy_aggregate_transports(**ctx):
    """Simule l'agrégation des transports"""
    logger.info('✓ Transports agrégés du jour précédent')
    ctx['ti'].xcom_push(key='transport_count', value=150)

def dummy_calculate_kpis(**ctx):
    """Simule le calcul des KPIs"""
    logger.info('✓ KPIs calculés')
    
    kpis = {
        'cout_moyen_tonne': 385.50,
        'taux_perte_moyen': 0.0325,
        'tonnage_total': 4250.5,
    }
    
    for key, value in kpis.items():
        ctx['ti'].xcom_push(key=key, value=value)
    
    logger.info(f'  Coût moyen/tonne : {kpis["cout_moyen_tonne"]:.2f} FCFA')
    logger.info(f'  Taux perte moyen : {kpis["taux_perte_moyen"]:.2%}')
    logger.info(f'  Tonnage total    : {kpis["tonnage_total"]:.1f} t')

def check_deviation(**ctx):
    """Vérifie la déviation du modèle"""
    # Simulation : déviation aléatoire
    import random
    deviation = random.uniform(0.02, 0.08)
    
    logger.info(f'Déviation modèle mesurée : {deviation:.4f}')
    ctx['ti'].xcom_push(key='deviation', value=deviation)
    
    # Décider si on retraine
    if deviation > 0.05:
        logger.info('⚠ Déviation > 5% → Réentraînement recommandé')
        return 'retrain_model'
    else:
        logger.info('✓ Déviation acceptable → Skip réentraînement')
        return 'skip_retrain'

def retrain_model(**ctx):
    """Réentraîne le modèle Random Forest"""
    logger.info('Lancement du réentraînement...')
    
    # En production : spark-submit
    # result = subprocess.run([
    #     'spark-submit', '--master', 'spark://spark-master:7077',
    #     'src/train_perte_model.py',
    #     '--output-path', 'models/perte_model_latest',
    #     '--window-days', '90'
    # ], capture_output=True, text=True, timeout=3600)
    
    # Pour le TP : simulation
    logger.info('Entraînement Random Forest : 100 arbres')
    logger.info('  RMSE = 0.0456')
    logger.info('  R²   = 0.8923')
    logger.info('✓ Modèle mis à jour')

# Créer le DAG
with DAG(
    'logi_agri_sn_pipeline',
    default_args=default_args,
    description='KPI + Réentraînement Logi-Agri SN',
    schedule_interval='0 3 * * *',  # Chaque jour à 3h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=['logi-agri', 'mlops', 'supply-chain']
) as dag:
    
    # Tâches
    start = DummyOperator(task_id='start')
    skip = DummyOperator(task_id='skip_retrain')
    end = DummyOperator(task_id='end')
    
    t1 = PythonOperator(
        task_id='aggregate_transports',
        python_callable=dummy_aggregate_transports,
        provide_context=True
    )
    
    t2 = PythonOperator(
        task_id='calculate_kpis',
        python_callable=dummy_calculate_kpis,
        provide_context=True
    )
    
    branch = BranchPythonOperator(
        task_id='check_deviation',
        python_callable=check_deviation,
        provide_context=True
    )
    
    t3 = PythonOperator(
        task_id='retrain_model',
        python_callable=retrain_model,
        provide_context=True
    )
    
    # Définir les dépendances
    start >> t1 >> t2 >> branch >> [t3, skip]
    t3 >> end
    skip >> end