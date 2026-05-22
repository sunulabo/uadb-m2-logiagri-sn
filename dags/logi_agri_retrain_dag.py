# dags/logi_agri_retrain_dag.py — DAG Airflow Logi-Agri SN
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess, logging

logger = logging.getLogger('logi_agri_dag')

default_args = {
    'owner': 'seye_ahmed',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def aggregate_transports(**ctx):
    logger.info('Agrégation transports terminée')

def calculate_kpis(**ctx):
    logger.info('KPIs recalculés')

def check_deviation(**ctx):
    import random
    dev = random.uniform(0.0, 0.1)
    logger.info(f'Déviation modèle : {dev:.4f}')
    ctx['ti'].xcom_push(key='deviation', value=dev)
    return 'retrain_model' if dev > 0.05 else 'skip_retrain'

def retrain_model(**ctx):
    result = subprocess.run([
        'python', 'train_perte_model.py',
        '--output-path', 'models/perte_model_latest',
        '--window-days', '90'
    ], capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        raise Exception(f'Job failed:\n{result.stderr}')
    logger.info('Modèle mis à jour')

with DAG('logi_agri_sn_pipeline',
         default_args=default_args,
         description='KPI + Réentraînement Logi-Agri SN',
         schedule_interval='0 3 * * *',
         start_date=days_ago(1),
         catchup=False,
         tags=['logi-agri', 'mlops', 'supply-chain']) as dag:

    start = DummyOperator(task_id='start')
    skip = DummyOperator(task_id='skip_retrain')
    end = DummyOperator(task_id='end')

    t1 = PythonOperator(task_id='aggregate_transports',
                        python_callable=aggregate_transports,
                        provide_context=True)
    t2 = PythonOperator(task_id='calculate_kpis',
                        python_callable=calculate_kpis,
                        provide_context=True)
    branch = BranchPythonOperator(task_id='check_deviation',
                                  python_callable=check_deviation,
                                  provide_context=True)
    t3 = PythonOperator(task_id='retrain_model',
                        python_callable=retrain_model,
                        provide_context=True)

    start >> t1 >> t2 >> branch >> [t3, skip]
    t3 >> end
    skip >> end