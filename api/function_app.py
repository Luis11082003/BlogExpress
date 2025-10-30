import azure.functions as func
import json
import pandas as pd
import mysql.connector
from datetime import datetime
import os
import io
import openpyxl
from dotenv import load_dotenv

load_dotenv()

# Configuración de base de datos (igual que antes)
def get_db_config():
    return {
        'host': os.getenv('DBHOST'),
        'user': os.getenv('DBUSER'),
        'password': os.getenv('DBPASSWORD'),
        'database': os.getenv('DBNAME'),
        'port': int(os.getenv('DBPORT', 3306)),
        'ssl_ca': None,
        'ssl_verify_cert': True
    }

def get_db_connection():
    try:
        config = get_db_config()
        return mysql.connector.connect(**config)
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

# Inicializar base de datos
def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registros_actualizacion (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    dia INT,
                    mes VARCHAR(50),
                    ano INT,
                    numero_publicacion INT,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    fecha_actualizacion DATETIME NOT NULL,
                    usuario VARCHAR(100),
                    cantidad_registros INT,
                    ip_cliente VARCHAR(45),
                    estado VARCHAR(20) DEFAULT 'completado',
                    modo_ejecucion VARCHAR(20) DEFAULT 'azure'
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historial_contenido (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    registro_id INT,
                    dia INT,
                    mes VARCHAR(50),
                    ano INT,
                    numero_publicacion INT,
                    tipo_contenido VARCHAR(10),
                    contenido TEXT,
                    estilo TEXT,
                    fecha_creacion DATETIME,
                    FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id) ON DELETE CASCADE
                )
            ''')
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error inicializando BD: {e}")

# API Routes
app = func.FunctionApp()

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener contenido del blog"""
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT h.dia, h.mes, h.ano, h.numero_publicacion, 
                   h.tipo_contenido, h.contenido, h.estilo
            FROM historial_contenido h
            ORDER BY h.fecha_creacion DESC
            LIMIT 50
        ''')
        contenido = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps(contenido),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="subir", methods=["POST"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    """Subir archivo CSV/Excel"""
    try:
        # Obtener archivo del request
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file:
            return func.HttpResponse(
                json.dumps({"error": "No se proporcionó archivo"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Leer archivo
        file_content = file.read()
        file_extension = file.filename.split('.')[-1].lower()
        
        if file_extension == 'csv':
            df = pd.read_csv(io.BytesIO(file_content))
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            return func.HttpResponse(
                json.dumps({"error": "Formato no soportado"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Guardar en base de datos
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor()
        
        # Insertar registro principal
        cursor.execute('''
            INSERT INTO registros_actualizacion 
            (nombre_archivo, fecha_actualizacion, usuario, cantidad_registros, modo_ejecucion)
            VALUES (%s, %s, %s, %s, %s)
        ''', (file.filename, datetime.now(), usuario, len(df), 'azure'))
        
        registro_id = cursor.lastrowid
        
        # Insertar contenido
        for index, row in df.iterrows():
            cursor.execute('''
                INSERT INTO historial_contenido 
                (registro_id, dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo, fecha_creacion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                registro_id,
                row.get('Día'), row.get('Mes'), row.get('Año'), row.get('N° Publicación'),
                row.get('Tipo'), row.get('Contenido / URL'), row.get('Estilo'), datetime.now()
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps({"success": True, "registro_id": registro_id}),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="historial", methods=["GET"])
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener historial de actualizaciones"""
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT id, dia, mes, ano, numero_publicacion, nombre_archivo, 
                   fecha_actualizacion, usuario, cantidad_registros, ip_cliente, estado
            FROM registros_actualizacion 
            ORDER BY fecha_actualizacion DESC 
            LIMIT 50
        ''')
        historial = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps(historial),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="detalle/{id}", methods=["GET"])
def get_detalle(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener detalle de un registro"""
    try:
        registro_id = req.route_params.get('id')
        
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM registros_actualizacion WHERE id = %s', (registro_id,))
        registro = cursor.fetchone()
        
        cursor.execute('SELECT * FROM historial_contenido WHERE registro_id = %s', (registro_id,))
        contenido = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps({"registro": registro, "contenido": contenido}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# Inicializar BD al cargar
init_db()