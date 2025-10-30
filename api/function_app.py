import azure.functions as func
import json
import mysql.connector
import os
import logging
import csv
import io
from datetime import datetime

# Configuración mejorada de base de datos
def get_db_config():
    config = {
        'host': os.environ.get('DBHOST', 'blog-rapido-express-mysql.mysql.database.azure.com'),
        'user': os.environ.get('DBUSER', 'admin_blog'),
        'password': os.environ.get('DBPASSWORD', 'PasswordSeguro123!'),
        'database': os.environ.get('DBNAME', 'blog-rapido-express-mysql'),
        'port': int(os.environ.get('DBPORT', '3306')),
        'ssl_disabled': True,
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci',
        'connect_timeout': 30,
        'buffered': True
    }
    logging.info(f"🔧 Config BD: host={config['host']}, user={config['user']}, db={config['database']}")
    return config

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        logging.info("✅ Conexión MySQL exitosa")
        return conn
    except Exception as e:
        logging.error(f"❌ Error conexión BD: {str(e)}")
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        logging.error("❌ No se pudo conectar para inicializar BD")
        return
    
    try:
        cursor = conn.cursor()
        
        # Tabla de registros (simplificada)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registros_actualizacion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre_archivo VARCHAR(255) NOT NULL,
                usuario VARCHAR(100) DEFAULT 'Anónimo',
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                cantidad_registros INT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # Tabla de contenido (simplificada)
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
                FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        conn.commit()
        logging.info("✅ Tablas de BD creadas/verificadas")
        
    except Exception as e:
        logging.error(f"❌ Error creando tablas: {str(e)}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Inicializar BD al cargar
init_db()

app = func.FunctionApp()

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        db_status = "connected" if conn else "disconnected"
        
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total FROM blog_contenido")
            total_elementos = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        else:
            total_elementos = 0
        
        return func.HttpResponse(
            json.dumps({
                "status": "healthy", 
                "database": db_status,
                "total_elementos": total_elementos,
                "timestamp": datetime.now().isoformat()
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "No se pudo conectar a la base de datos"}),
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
        
        return func.HttpResponse(
            json.dumps(contenido, default=str),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"Error en /blog: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
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
        required_columns = ['Día', 'Mes', 'Año', 'N° Publicación', 'Tipo', 'Contenido / URL']
        
        for row_num, row in enumerate(reader, 1):
            # Verificar columnas requeridas
            missing_columns = [col for col in required_columns if col not in row]
            if missing_columns:
                raise ValueError(f"Faltan columnas: {', '.join(missing_columns)} en fila {row_num}")
            
            # Procesar cada campo con validación
            try:
                dia = int(row['Día'].strip()) if row['Día'].strip() else 30
            except:
                dia = 30
                
            mes = row['Mes'].strip() if row['Mes'].strip() else 'Octubre'
            
            try:
                ano = int(row['Año'].strip()) if row['Año'].strip() else 2024
            except:
                ano = 2024
                
            try:
                numero_publicacion = int(row['N° Publicación'].strip()) if row['N° Publicación'].strip() else 1
            except:
                numero_publicacion = 1
            
            tipo_contenido = row['Tipo'].strip().upper() if row['Tipo'].strip() else 'P'
            contenido_texto = row['Contenido / URL'].strip() if row['Contenido / URL'].strip() else ''
            estilo = row.get('Estilo', '').strip()
            
            # Validar tipo de contenido
            if tipo_contenido not in ['T', 'ST', 'P', 'I']:
                raise ValueError(f"Fila {row_num}: Tipo '{tipo_contenido}' inválido. Use T, ST, P o I")
            
            # Validar contenido para imágenes
            if tipo_contenido == 'I' and not contenido_texto.startswith(('http://', 'https://')):
                raise ValueError(f"Fila {row_num}: Las imágenes deben contener URL válida")
            
            elemento = {
                'dia': dia,
                'mes': mes,
                'ano': ano,
                'numero_publicacion': numero_publicacion,
                'tipo_contenido': tipo_contenido,
                'contenido': contenido_texto,
                'estilo': estilo
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
                json.dumps({"error": "No se recibió ningún archivo"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file or not file.filename:
            return func.HttpResponse(
                json.dumps({"error": "No se proporcionó un archivo válido"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        if not file.filename.lower().endswith('.csv'):
            return func.HttpResponse(
                json.dumps({"error": "Solo se permiten archivos CSV"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Procesar archivo
        file_content = file.read()
        elementos_procesados = procesar_csv(file_content)
        
        if not elementos_procesados:
            return func.HttpResponse(
                json.dumps({"error": "El archivo CSV está vacío o no contiene datos válidos"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Guardar en base de datos
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "No se pudo conectar a la base de datos"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        try:
            cursor = conn.cursor()
            
            # Insertar registro principal
            cursor.execute('''
                INSERT INTO registros_actualizacion 
                (nombre_archivo, usuario, cantidad_registros)
                VALUES (%s, %s, %s)
            ''', (file.filename, usuario, len(elementos_procesados)))
            
            registro_id = cursor.lastrowid
            
            # Insertar cada elemento del contenido
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
            
            # Contar tipos de contenido para el response
            tipos = {'T': 0, 'ST': 0, 'P': 0, 'I': 0}
            for elemento in elementos_procesados:
                if elemento['tipo_contenido'] in tipos:
                    tipos[elemento['tipo_contenido']] += 1
            
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "registro_id": registro_id,
                    "mensaje": f"✅ Archivo procesado exitosamente. {len(elementos_procesados)} elementos cargados.",
                    "elementos_procesados": len(elementos_procesados),
                    "tipos_contenido": tipos
                }),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=400,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        logging.error(f"Error en /subir: {str(e)}")
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
                json.dumps({"error": "No se pudo conectar a la base de datos"}),
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
        logging.error(f"Error en /historial: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )