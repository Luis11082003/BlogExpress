import azure.functions as func
import json
import mysql.connector
import os
import logging
import csv
import io
from datetime import datetime

# Configuración de base de datos
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
        logging.error(f"❌ Error conexión BD: {e}")
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
                    nombre_archivo VARCHAR(255),
                    usuario VARCHAR(100),
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    cantidad_registros INT
                )
            ''')
            
            # Tabla de contenido
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blog_contenido (
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
                    FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id)
                )
            ''')
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Error BD: {e}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

# Inicializar BD
init_db()

app = func.FunctionApp()

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"status": "healthy", "message": "Sistema funcionando"}),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo
            FROM blog_contenido 
            ORDER BY fecha_creacion DESC
            LIMIT 100
        ''')
        contenido = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if contenido:
            return func.HttpResponse(
                json.dumps(contenido, default=str),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        else:
            # Si no hay datos, devolver vacío
            return func.HttpResponse(
                json.dumps([]),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

def procesar_csv(file_content):
    """Procesar CSV manualmente"""
    try:
        content_str = file_content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(content_str))
        
        elementos_procesados = []
        
        for row in reader:
            # Normalizar nombres de columnas
            dia = row.get('Día') or row.get('dia') or '30'
            mes = row.get('Mes') or row.get('mes') or 'Octubre'
            ano = row.get('Año') or row.get('ano') or '2024'
            numero_pub = row.get('N° Publicación') or row.get('numero_publicacion') or '1'
            tipo = row.get('Tipo') or row.get('tipo_contenido') or 'P'
            contenido = row.get('Contenido / URL') or row.get('contenido') or ''
            estilo = row.get('Estilo') or row.get('estilo') or ''
            
            elemento = {
                'dia': int(dia) if dia.strip() else 30,
                'mes': mes.strip(),
                'ano': int(ano) if ano.strip() else 2024,
                'numero_publicacion': int(numero_pub) if numero_pub.strip() else 1,
                'tipo_contenido': tipo.strip().upper(),
                'contenido': contenido.strip(),
                'estilo': estilo.strip()
            }
            
            elementos_procesados.append(elemento)
        
        return elementos_procesados
        
    except Exception as e:
        raise ValueError(f"Error procesando CSV: {str(e)}")

@app.route(route="subir", methods=["POST", "OPTIONS"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            }
        )
    
    try:
        if not req.files:
            return func.HttpResponse(
                json.dumps({"error": "No se recibió archivo"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file or not file.filename:
            return func.HttpResponse(
                json.dumps({"error": "Archivo inválido"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Procesar archivo
        file_content = file.read()
        elementos_procesados = procesar_csv(file_content)
        
        if not elementos_procesados:
            return func.HttpResponse(
                json.dumps({"error": "El archivo está vacío o no tiene formato correcto"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Guardar en BD
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a base de datos"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        cursor = conn.cursor()
        
        # Insertar registro principal
        cursor.execute('''
            INSERT INTO registros_actualizacion 
            (nombre_archivo, usuario, cantidad_registros)
            VALUES (%s, %s, %s)
        ''', (file.filename, usuario, len(elementos_procesados)))
        
        registro_id = cursor.lastrowid
        
        # Insertar contenido
        for elemento in elementos_procesados:
            cursor.execute('''
                INSERT INTO blog_contenido 
                (registro_id, dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                registro_id,
                elemento['dia'],
                elemento['mes'],
                elemento['ano'],
                elemento['numero_publicacion'],
                elemento['tipo_contenido'],
                elemento['contenido'],
                elemento['estilo']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Contar tipos de contenido
        tipos = {'T': 0, 'ST': 0, 'P': 0, 'I': 0}
        for elemento in elementos_procesados:
            if elemento['tipo_contenido'] in tipos:
                tipos[elemento['tipo_contenido']] += 1
        
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "registro_id": registro_id,
                "mensaje": f"✅ Archivo procesado: {len(elementos_procesados)} elementos cargados",
                "elementos_procesados": len(elementos_procesados),
                "tipos_contenido": tipos
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
            
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=400,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        logging.error(f"Error general: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.route(route="historial", methods=["GET"])
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a BD"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT id, nombre_archivo, usuario, fecha_actualizacion, cantidad_registros
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
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )