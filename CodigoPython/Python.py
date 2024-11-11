import pandas as pd
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
from sqlalchemy.exc import ProgrammingError
import os
import psutil
import time
import threading
import tkinter as tk

# Parámetros de conexión a SQL Server usando Windows Authentication
server = 'DESKTOP-7U12N0I'
database = 'Python'
table_name = 'wikipedia_collection'

# Crear la conexión a SQL Server con pyodbc y Windows Authentication
connection_string = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes"
engine = create_engine(connection_string)

# Definir el número de hilos (threads) de acuerdo con el número de núcleos de la CPU
max_workers = min(6, os.cpu_count())

# Establecer prioridad alta en Windows
p = psutil.Process(os.getpid())
p.nice(psutil.REALTIME_PRIORITY_CLASS)

# Configuración de pausa/reanudación
pause_event = threading.Event()
pause_event.set()  # Iniciar en estado 'sin pausa'

# Variables para el tiempo de ejecución
start_time = None
elapsed_time = 0

# Definir la estructura de la tabla
def create_table_if_not_exists():
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('content_string', String, nullable=False),
        Column('article_title', String),
        Column('full_section_title', String),
        Column('block_type', String),
        Column('language', String),
        Column('last_edit_date', String),
        Column('num_tokens', Integer),
        Column('unique_id', String)
    )
    
    try:
        metadata.create_all(engine)
    except ProgrammingError as e:
        print("Error al crear la tabla:", e)

# Función para traducir el contenido
def translate_content(content):
    try:
        return GoogleTranslator(source='auto', target='es').translate(content)
    except Exception:
        return content

# Procesa el chunk: traduce y actualiza la columna 'language'
def process_chunk(chunk):
    # Asegurarse de que no haya valores nulos en 'content_string'
    chunk['content_string'] = chunk['content_string'].fillna("Contenido no disponible")
    
    # Traducir el contenido
    chunk['content_string'] = chunk['content_string'].apply(translate_content)
    
    # Actualizar la columna 'language' a 'es'
    chunk['language'] = 'es'
    return chunk

# Función para cargar los datos procesados en SQL Server
def upload_to_sql(chunk):
    try:
        # Verificar y reemplazar valores nulos en 'content_string' antes de la carga
        chunk['content_string'] = chunk['content_string'].fillna("Contenido no disponible")

        # Excluir la columna 'id' antes de la carga
        chunk = chunk.drop(columns=['id'], errors='ignore')
        chunk.to_sql(table_name, engine, if_exists='append', index=False)
    except Exception as e:
        print("Error al cargar el chunk en SQL Server:", e)

# Temporizador en tiempo real y barra de progreso
def display_progress(total_rows, processed_rows):
    global start_time, elapsed_time
    start_time = time.time()
    
    while processed_rows[0] < total_rows:
        pause_event.wait()  # Espera si está en pausa

        # Actualiza el tiempo solo si el proceso no está en pausa
        if pause_event.is_set():
            elapsed_time += time.time() - start_time
            start_time = time.time()  # Resetea start_time para el próximo incremento de tiempo
        
        percent_complete = (processed_rows[0] / total_rows) * 100
        print(f"\rTiempo transcurrido: {int(elapsed_time)}s | Progreso: {percent_complete:.2f}% completado", end="")
        time.sleep(1)

# Procesamiento y carga de datos en chunks en paralelo
def process_and_upload(file_path, chunk_size=10_000):
    total_rows = 1_000_000  # Asumimos un total de 1 millón de filas
    processed_rows = [0]  # Lista mutable para rastrear las filas procesadas

    # Iniciar el hilo para mostrar el tiempo transcurrido y el progreso
    progress_thread = threading.Thread(target=display_progress, args=(total_rows, processed_rows))
    progress_thread.daemon = True
    progress_thread.start()

    # Crear la tabla si no existe
    create_table_if_not_exists()

    # Leer en bloques (chunks) desde el archivo CSV con codificación utf-8
    data_iterator = pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8')

    # Procesa y sube cada chunk en paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk in data_iterator:
            pause_event.wait()  # Pausa la carga si está en pausa

            # Procesar y cargar cada chunk en un hilo separado
            future = executor.submit(process_and_upload_chunk, chunk)
            futures.append(future)

            # Actualizar la cantidad de filas procesadas
            processed_rows[0] += len(chunk)

            # Espera si ya hay demasiados futuros pendientes para evitar sobrecarga de memoria
            if len(futures) >= max_workers:
                for future in as_completed(futures):
                    future.result()  # Asegura que los hilos se completen
                futures = []  # Reinicia la lista de futuros pendientes

        # Asegurarse de que todos los hilos se completen al final
        for future in as_completed(futures):
            future.result()

    # Calcular el tiempo total de ejecución
    end_time = time.time()
    total_time = elapsed_time  # Tiempo total de ejecución es el tiempo acumulado real
    print(f"\nTiempo total de ejecución: {total_time:.2f} segundos")

    # Si se han procesado todos los registros, imprime el tiempo total
    if processed_rows[0] >= 10_000:
        print(f"\nTiempo total de ejecución para cargar 10,000 registros: {total_time:.2f} segundos")

# Procesa y carga un chunk específico
def process_and_upload_chunk(chunk):
    # Procesar el chunk
    processed_chunk = process_chunk(chunk)
    # Cargar el chunk procesado en SQL
    upload_to_sql(processed_chunk)

# Funciones para la interfaz gráfica
def pause_execution():
    global start_time
    pause_event.clear()

def resume_execution():
    global start_time
    pause_event.set()
    start_time = time.time()  # Reiniciar el tiempo de inicio al reanudar

# Interfaz gráfica con tkinter
def start_gui():
    root = tk.Tk()
    root.title("Control de Ejecución")

    pause_button = tk.Button(root, text="Pausar", command=pause_execution)
    pause_button.pack(pady=10)

    resume_button = tk.Button(root, text="Reanudar", command=resume_execution)
    resume_button.pack(pady=10)

    root.mainloop()

# Ejecutar el proceso y la interfaz gráfica
file_path = 'wikipedia_collection.csv'
threading.Thread(target=process_and_upload, args=(file_path,)).start()
start_gui()