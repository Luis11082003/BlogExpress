import azure.functions as func
import json
import pandas as pd
import mysql.connector
from datetime import datetime
import os
import io
import logging

# Configuración mejorada para Azure MySQL
def get_db_config():
    return {
        'host': os.environ.get('DBHOST', 'blog-rapido-express-mysql.mysql.database.azure.com'),
        'user': os.environ.get('DBUSER', 'admin_blog'),
        'password': os.environ.get('DBPASSWORD', 'PasswordSeguro123!'),
        'database': os.environ.get('DBNAME', 'blog-rapido-express-mysql'),
        'port': int(os.environ.get('DBPORT', '3306')),
        'ssl_disabled': True,
        'charset': 'utf8mb4',
        'connection_timeout': 30
    }

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        print(f"✅ Conectado a MySQL: {config['host']}")
        return conn
    except Exception as e:
        print(f"❌ Error de conexión a BD: {str(e)}")
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
            print("✅ Base de datos inicializada")
            
        except Exception as e:
            print(f"❌ Error BD: {e}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

# Procesar archivo - VERSIÓN SIMPLIFICADA Y ROBUSTA
def procesar_archivo(file_content, filename):
    try:
        # Leer archivo
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
        elif filename.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            return None, "Formato no soportado"
        
        # Verificar columnas mínimas
        required_columns = ['dia', 'mes', 'ano', 'numero_publicacion', 'tipo_contenido', 'contenido']
        df.columns = [col.strip().lower() for col in df.columns]
        
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            return None, f"Faltan columnas: {', '.join(missing)}"
        
        # Procesar filas
        contenido_procesado = []
        for index, row in df.iterrows():
            try:
                tipo = str(row['tipo_contenido']).strip().upper()
                contenido = str(row['contenido']).strip()
                estilo = str(row.get('estilo', '')).strip()
                
                # Validar tipo
                if tipo not in ['T', 'ST', 'P', 'I']:
                    return None, f"Fila {index+1}: Tipo '{tipo}' inválido. Use T, ST, P o I"
                
                # Validar imagen
                if tipo == 'I' and not contenido.startswith(('http://', 'https://')):
                    return None, f"Fila {index+1}: Las imágenes deben ser URLs válidas"
                
                contenido_procesado.append({
                    'dia': int(row['dia']) if pd.notna(row['dia']) else None,
                    'mes': str(row['mes']).strip(),
                    'ano': int(row['ano']) if pd.notna(row['ano']) else None,
                    'numero_publicacion': int(row['numero_publicacion']) if pd.notna(row['numero_publicacion']) else None,
                    'tipo_contenido': tipo,
                    'contenido': contenido,
                    'estilo': estilo
                })
                
            except Exception as e:
                return None, f"Error en fila {index+1}: {str(e)}"
        
        return contenido_procesado, None
        
    except Exception as e:
        return None, f"Error procesando archivo: {str(e)}"

# Azure Functions App
app = func.FunctionApp()

@app.route(route="api/blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error conectando a BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
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
        
        return func.HttpResponse(
            json.dumps(contenido, default=str),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="api/subir", methods=["POST"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if not req.files:
            return func.HttpResponse(
                json.dumps({"error": "No se recibió archivo"}),
                status_code=400,
                mimetype="application/json"
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file:
            return func.HttpResponse(
                json.dumps({"error": "Archivo inválido"}),
                status_code=400,
                mimetype="application/json"
            )
        
        print(f"📤 Procesando: {file.filename}")
        file_content = file.read()
        
        contenido_procesado, error = procesar_archivo(file_content, file.filename)
        if error:
            return func.HttpResponse(
                json.dumps({"error": error}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Guardar en BD
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
        try:
            cursor = conn.cursor()
            
            # Insertar registro
            primer_item = contenido_procesado[0]
            cursor.execute('''
                INSERT INTO registros_actualizacion 
                (dia, mes, ano, numero_publicacion, nombre_archivo, usuario, cantidad_registros)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                primer_item['dia'],
                primer_item['mes'], 
                primer_item['ano'],
                primer_item['numero_publicacion'],
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
            
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "registro_id": registro_id,
                    "mensaje": f"✅ Archivo procesado: {len(contenido_procesado)} elementos",
                    "elementos_procesados": len(contenido_procesado)
                }),
                status_code=200,
                mimetype="application/json"
            )
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="api/historial", methods=["GET"])
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error BD"}),
                status_code=500,
                mimetype="application/json"
            )
        
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
        
        return func.HttpResponse(
            json.dumps(historial, default=str),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="api/health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({
            "status": "running", 
            "mode": "azure",
            "timestamp": datetime.now().isoformat()
        }),
        status_code=200,
        mimetype="application/json"
    )

# Inicializar al cargar
print("🚀 Iniciando Blog Express...")
init_db()