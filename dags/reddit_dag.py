# Añadiendo las librerías necesarias
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

# Añadir el directorio raíz al sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline



# Importar la función reddit_pipeline correctamente
try:
    from pipelines.reddit_pipeline import reddit_pipeline
    print(f"reddit_pipeline importado correctamente: {reddit_pipeline}, tipo: {type(reddit_pipeline)}")
except ImportError as e:
    print(f"Error al importar reddit_pipeline: {e}")
    reddit_pipeline = None  # Definir como None para evitar otros errores si la importación falla

# Verificar si reddit_pipeline es una función
if callable(reddit_pipeline):
    print("reddit_pipeline es callable")
else:
    print("reddit_pipeline NO es callable o no se pudo importar correctamente")

# Definición de los argumentos por defecto
default_args = {
    'owner': 'Hedinyer Perucho',
    'start_date': datetime(2024, 10, 4)
}

file_postfix = datetime.now().strftime("%Y%m%d")

# Definir el DAG usando el contexto `with`
with DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
) as dag:
    # Definir la tarea de PythonOperator solo si reddit_pipeline es callable
    if callable(reddit_pipeline):
        extract = PythonOperator(
            task_id='reddit_extraction',
            python_callable=reddit_pipeline,  # Asegúrate de que esto es una función válida
            op_kwargs={
                'file_name': f'reddit_{file_postfix}',
                'subreddit': 'dataengineering',
                'time_filter': 'day',
                'limit': 100
            },
            dag= dag
        )
    else:
        print("No se pudo definir la tarea extract porque reddit_pipeline no es callable.")


# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract >> upload_s3