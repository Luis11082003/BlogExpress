import azure.functions as func
import json
import mysql.connector
import os
import logging
import csv
import io
from datetime import datetime

# Configuración ULTRA-ROBUSTA de base de datos
def get_db_config():
    return {
        'host': os.environ.get('DBHOST', 'blog-rapido-express-mysql.mysql.database.azure.com'),
        'user': os.environ.get('DBUSER', 'admin_blog'),
        'password': os.environ.get('DBPASSWORD', 'PasswordSeguro123!'),
        'database': os.environ.get('DBNAME', 'blog-rapido-express-mysql'),
        'port': int(os.environ.get('DBPORT', '3306')),
        'ssl_disabled': True,
        'charset': 'utf8mb4',
        'connect_timeout': 60,
        'buffered': True,
        'use_pure': True,  # Importante para Azure
        'pool_size': 5,
        'pool_reset_session': True
    }

def get_db_connection():
    max_retries = 3
    retry_delay = 2  # segundos
    
    for attempt in range(max_retries):
        try:
            config = get_db_config()
            conn = mysql.connector.connect(**config)
            logging.info(f"Conexion MySQL exitosa (intento {attempt + 1})")
            return conn
        except mysql.connector.Error as e:
            logging.error(f"Intento {attempt + 1} fallado: {str(e)}")
            if attempt == max_retries - 1:
                logging.error("Todos los intentos de conexion fallaron")
                return None
            # Esperar antes de reintentar (implementación básica)
            import time
            time.sleep(retry_delay)
        except Exception as e:
            logging.error(f"Error inesperado en conexion: {str(e)}")
            return None

def init_db():
    conn = get_db_connection()
    if not conn:
        logging.error("No se pudo conectar para inicializar BD")
        return
    
    try:
        cursor = conn.cursor()
        
        # Tabla de registros (simplificada)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registros_actualizacion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre_archivo VARCHAR(255) NOT NULL,
                usuario VARCHAR(100) DEFAULT 'Anonimo',
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                cantidad_registros INT DEFAULT 0,
                INDEX idx_fecha (fecha_actualizacion)
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
                FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id) ON DELETE CASCADE,
                INDEX idx_registro (registro_id),
                INDEX idx_fecha_creacion (fecha_creacion),
                INDEX idx_tipo (tipo_contenido)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        conn.commit()
        logging.info("Tablas de BD creadas/verificadas")
        
    except Exception as e:
        logging.error(f"Error creando tablas: {str(e)}")
        conn.rollback()
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
        
        total_elementos = 0
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as total FROM blog_contenido")
                total_elementos = cursor.fetchone()[0]
                cursor.close()
            except Exception as e:
                logging.error(f"Error contando elementos: {str(e)}")
            finally:
                if conn.is_connected():
                    conn.close()
        
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
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo
                FROM blog_contenido 
                ORDER BY fecha_creacion DESC
                LIMIT 100
            ''')
            contenido = cursor.fetchall()
            
            return func.HttpResponse(
                json.dumps(contenido, default=str),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
        except Exception as e:
            logging.error(f"Error ejecutando consulta: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": "Error en la consulta de datos"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                
    except Exception as e:
        logging.error(f"Error en /blog: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

def procesar_csv(file_content):
    """Procesar CSV manualmente con validacion robusta"""
    try:
        content_str = file_content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(content_str))
        
        elementos_procesados = []
        required_columns = ['Dia', 'Mes', 'Ano', 'N° Publicacion', 'Tipo', 'Contenido / URL']
        
        for row_num, row in enumerate(reader, 1):
            # Verificar columnas requeridas
            missing_columns = [col for col in required_columns if col not in row]
            if missing_columns:
                raise ValueError(f"Faltan columnas: {', '.join(missing_columns)} en fila {row_num}")
            
            # Procesar cada campo con validacion robusta
            try:
                dia = int(row['Dia'].strip()) if row['Dia'].strip() else 30
            except (ValueError, TypeError):
                dia = 30
                
            mes = row['Mes'].strip() if row['Mes'].strip() else 'Octubre'
            
            try:
                ano = int(row['Ano'].strip()) if row['Ano'].strip() else 2024
            except (ValueError, TypeError):
                ano = 2024
                
            try:
                numero_publicacion = int(row['N° Publicacion'].strip()) if row['N° Publicacion'].strip() else 1
            except (ValueError, TypeError):
                numero_publicacion = 1
            
            tipo_contenido = row['Tipo'].strip().upper() if row['Tipo'].strip() else 'P'
            contenido_texto = row['Contenido / URL'].strip() if row['Contenido / URL'].strip() else ''
            estilo = row.get('Estilo', '').strip()
            
            # Validar tipo de contenido
            if tipo_contenido not in ['T', 'ST', 'P', 'I']:
                raise ValueError(f"Fila {row_num}: Tipo '{tipo_contenido}' invalido. Use T, ST, P o I")
            
            # Validar contenido para imagenes
            if tipo_contenido == 'I' and not contenido_texto.startswith(('http://', 'https://')):
                raise ValueError(f"Fila {row_num}: Las imagenes deben contener URL valida")
            
            # Validar contenido no vacio para otros tipos
            if tipo_contenido != 'I' and not contenido_texto:
                raise ValueError(f"Fila {row_num}: El contenido no puede estar vacio")
            
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
        
    except UnicodeDecodeError:
        # Intentar con diferentes codificaciones
        try:
            content_str = file_content.decode('latin-1')
            reader = csv.DictReader(io.StringIO(content_str))
            # Reprocesar con latin-1...
            # (implementacion similar a la anterior)
        except Exception as e:
            raise ValueError(f"Error de codificacion del archivo: {str(e)}")
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
                json.dumps({"error": "No se recibio ningun archivo"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anonimo')
        
        if not file or not file.filename:
            return func.HttpResponse(
                json.dumps({"error": "No se proporciono un archivo valido"}),
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
        
        # Validar tamaño del archivo (max 10MB)
        file_content = file.read()
        if len(file_content) > 10 * 1024 * 1024:
            return func.HttpResponse(
                json.dumps({"error": "El archivo es demasiado grande (maximo 10MB)"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Procesar archivo
        elementos_procesados = procesar_csv(file_content)
        
        if not elementos_procesados:
            return func.HttpResponse(
                json.dumps({"error": "El archivo CSV esta vacio o no contiene datos validos"}),
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
                    "mensaje": f"Archivo procesado exitosamente. {len(elementos_procesados)} elementos cargados.",
                    "elementos_procesados": len(elementos_procesados),
                    "tipos_contenido": tipos
                }),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error en transaccion BD: {str(e)}")
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
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT id, nombre_archivo, usuario, fecha_actualizacion, cantidad_registros
                FROM registros_actualizacion 
                ORDER BY fecha_actualizacion DESC 
                LIMIT 50
            ''')
            historial = cursor.fetchall()
            
            return func.HttpResponse(
                json.dumps(historial, default=str),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        except Exception as e:
            logging.error(f"Error consultando historial: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": "Error al obtener el historial"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                
    except Exception as e:
        logging.error(f"Error en /historial: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )