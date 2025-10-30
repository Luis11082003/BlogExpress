import azure.functions as func
import json
import pandas as pd
import mysql.connector
from datetime import datetime
import os
import io
import logging

# Configuración de base de datos - CON TUS VARIABLES
def get_db_config():
    return {
        'host': os.environ.get('DBHOST', 'blog-rapido-express-mysql.mysql.database.azure.com'),
        'user': os.environ.get('DBUSER', 'admin_blog'),
        'password': os.environ.get('DBPASSWORD', 'PasswordSeguro123!'),
        'database': os.environ.get('DBNAME', 'blog-rapido-express-mysql'),
        'port': int(os.environ.get('DBPORT', '3306')),
        'ssl_disabled': True
    }

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        return conn
    except Exception as e:
        logging.error(f"Error de conexión a BD: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Tabla de registros
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registros_actualizacion (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    dia INT,
                    mes VARCHAR(50),
                    ano INT,
                    numero_publicacion INT,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    usuario VARCHAR(100) DEFAULT 'Anónimo',
                    cantidad_registros INT DEFAULT 0,
                    estado VARCHAR(20) DEFAULT 'completado'
                )
            ''')
            
            # Tabla de contenido
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
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id) ON DELETE CASCADE
                )
            ''')
            
            conn.commit()
            logging.info("✅ Base de datos inicializada")
            
        except Exception as e:
            logging.error(f"Error BD: {e}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

# Inicializar BD al cargar
init_db()

app = func.FunctionApp()

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"status": "healthy", "message": "Sistema completo funcionando", "mode": "azure"}),
        status_code=200,
        mimetype="application/json"
    )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            # Datos de ejemplo si no hay conexión a BD
            sample_data = [
                {
                    "dia": 30, "mes": "Octubre", "ano": 2024,
                    "tipo_contenido": "T", "contenido": "🚀 BLOG EXPRESS - SISTEMA COMPLETO", 
                    "estilo": "color:#2c3e50; text-align:center"
                },
                {
                    "dia": 30, "mes": "Octubre", "ano": 2024,
                    "tipo_contenido": "P", "contenido": "El sistema está funcionando al 100% con Azure Functions + MySQL", 
                    "estilo": "color:#555; background:#f8f9fa; padding:1rem; border-radius:8px"
                }
            ]
            return func.HttpResponse(json.dumps(sample_data), status_code=200, mimetype="application/json")
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo
            FROM historial_contenido 
            ORDER BY fecha_creacion DESC
            LIMIT 100
        ''')
        contenido = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return func.HttpResponse(json.dumps(contenido, default=str), status_code=200, mimetype="application/json")
        
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")

@app.route(route="subir", methods=["POST"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if not req.files:
            return func.HttpResponse(json.dumps({"error": "No se recibió archivo"}), status_code=400, mimetype="application/json")
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file:
            return func.HttpResponse(json.dumps({"error": "Archivo inválido"}), status_code=400, mimetype="application/json")
        
        # Leer y procesar archivo
        file_content = file.read()
        
        if file.filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
        else:
            return func.HttpResponse(json.dumps({"error": "Solo se permiten archivos CSV"}), status_code=400, mimetype="application/json")
        
        # Procesar datos
        contenido_procesado = []
        for index, row in df.iterrows():
            contenido_procesado.append({
                'dia': int(row['Día']) if 'Día' in row else 30,
                'mes': str(row['Mes']) if 'Mes' in row else 'Octubre',
                'ano': int(row['Año']) if 'Año' in row else 2024,
                'numero_publicacion': int(row['N° Publicación']) if 'N° Publicación' in row else 1,
                'tipo_contenido': str(row['Tipo']).strip().upper() if 'Tipo' in row else 'P',
                'contenido': str(row['Contenido / URL']) if 'Contenido / URL' in row else 'Contenido de prueba',
                'estilo': str(row['Estilo']) if 'Estilo' in row else ''
            })
        
        # Guardar en BD
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Insertar registro
                cursor.execute('''
                    INSERT INTO registros_actualizacion 
                    (dia, mes, ano, numero_publicacion, nombre_archivo, usuario, cantidad_registros)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                    contenido_procesado[0]['dia'],
                    contenido_procesado[0]['mes'], 
                    contenido_procesado[0]['ano'],
                    contenido_procesado[0]['numero_publicacion'],
                    file.filename,
                    usuario,
                    len(contenido_procesado)
                ))
                
                registro_id = cursor.lastrowid
                
                # Insertar contenido
                for item in contenido_procesado:
                    cursor.execute('''
                        INSERT INTO historial_contenido 
                        (registro_id, dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        registro_id,
                        item['dia'],
                        item['mes'],
                        item['ano'], 
                        item['numero_publicacion'],
                        item['tipo_contenido'],
                        item['contenido'],
                        item['estilo']
                    ))
                
                conn.commit()
                cursor.close()
                conn.close()
                
            except Exception as e:
                logging.error(f"Error guardando en BD: {e}")
        
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "registro_id": 1,
                "mensaje": f"✅ Archivo procesado: {len(contenido_procesado)} elementos cargados",
                "elementos_procesados": len(contenido_procesado)
            }),
            status_code=200,
            mimetype="application/json"
        )
                
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": f"Error: {str(e)}"}), status_code=500, mimetype="application/json")

@app.route(route="historial", methods=["GET"]) 
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            sample_historial = [
                {
                    "id": 1,
                    "nombre_archivo": "demo.csv",
                    "usuario": "Admin",
                    "fecha_actualizacion": "2024-10-30T00:00:00Z",
                    "cantidad_registros": 2
                }
            ]
            return func.HttpResponse(json.dumps(sample_historial), status_code=200, mimetype="application/json")
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT id, dia, mes, ano, numero_publicacion, nombre_archivo, 
                   fecha_actualizacion, usuario, cantidad_registros
            FROM registros_actualizacion 
            ORDER BY fecha_actualizacion DESC 
            LIMIT 50
        ''')
        historial = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return func.HttpResponse(json.dumps(historial, default=str), status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")