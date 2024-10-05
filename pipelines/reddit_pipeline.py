import os
import pandas as pd
from etls.reddit_etl import connect_reddit, extract_posts, load_data_to_csv, transform_data
from utils.constants import CLIENT_ID, OUTPUT_PATH, SECRET

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Imprimir el valor de OUTPUT_PATH para depuración
    print(f"OUTPUT_PATH es: {OUTPUT_PATH}")

    # Conectar a la instancia de Reddit
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    
    # Extracción de datos
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    
    # Transformación de datos
    post_df = transform_data(post_df)
    
    # Definir la ruta del archivo
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    
    # Crear el directorio si no existe
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Cargar los datos en un archivo CSV
    load_data_to_csv(post_df, file_path)

    return file_path
