import azure.functions as func
import json
import mysql.connector
import os
import logging
import csv
import io

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
        logging.info("✅ Conectado a MySQL")
        return conn
    except Exception as e:
        logging.error(f"❌ Error conexión BD: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blog_contenido (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    dia INT,
                    mes VARCHAR(50),
                    ano INT,
                    numero_publicacion INT,
                    tipo_contenido VARCHAR(10),
                    contenido TEXT,
                    estilo TEXT,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            logging.info("✅ Tabla blog_contenido lista")
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
        json.dumps({"status": "healthy", "message": "Sistema funcionando sin pandas"}),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SELECT * FROM blog_contenido ORDER BY fecha_creacion DESC LIMIT 50')
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
        
        # Si no hay datos en BD, devolver ejemplo
        data = [
            {
                "dia": 30, "mes": "Octubre", "ano": 2024, "numero_publicacion": 1,
                "tipo_contenido": "T", "contenido": "🚀 BLOG EXPRESS - SUBE UN ARCHIVO CSV", 
                "estilo": "color:#2c3e50; text-align:center"
            },
            {
                "dia": 30, "mes": "Octubre", "ano": 2024, "numero_publicacion": 1,
                "tipo_contenido": "P", "contenido": "Usa la página 'Subir' para cargar un archivo CSV con el formato correcto.", 
                "estilo": "color:#555; background:#f8f9fa; padding:1rem; border-radius:8px"
            }
        ]
        return func.HttpResponse(
            json.dumps(data),
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

def procesar_csv_manual(file_content):
    """Procesar CSV manualmente sin pandas"""
    try:
        # Convertir bytes a string
        content_str = file_content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(content_str))
        
        elementos_procesados = []
        required_columns = ['Día', 'Mes', 'Año', 'N° Publicación', 'Tipo', 'Contenido / URL']
        
        for row_num, row in enumerate(reader, 1):
            # Verificar columnas requeridas
            missing_columns = [col for col in required_columns if col not in row]
            if missing_columns:
                raise ValueError(f"Faltan columnas: {', '.join(missing_columns)}")
            
            # Procesar fila
            elemento = {
                'dia': int(row['Día']) if row['Día'].strip() else 30,
                'mes': row['Mes'].strip() or 'Octubre',
                'ano': int(row['Año']) if row['Año'].strip() else 2024,
                'numero_publicacion': int(row['N° Publicación']) if row['N° Publicación'].strip() else 1,
                'tipo_contenido': row['Tipo'].strip().upper(),
                'contenido': row['Contenido / URL'].strip(),
                'estilo': row.get('Estilo', '').strip()
            }
            
            # Validar tipo de contenido
            if elemento['tipo_contenido'] not in ['T', 'ST', 'P', 'I']:
                raise ValueError(f"Fila {row_num}: Tipo '{elemento['tipo_contenido']}' inválido. Use T, ST, P o I")
            
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
        
        if not file or not file.filename.lower().endswith('.csv'):
            return func.HttpResponse(
                json.dumps({"error": "Se requiere archivo CSV"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Leer y procesar archivo
        file_content = file.read()
        elementos_procesados = procesar_csv_manual(file_content)
        
        # Guardar en BD
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            for elemento in elementos_procesados:
                cursor.execute('''
                    INSERT INTO blog_contenido 
                    (dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                    elemento['dia'], elemento['mes'], elemento['ano'],
                    elemento['numero_publicacion'], elemento['tipo_contenido'],
                    elemento['contenido'], elemento['estilo']
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
                "message": f"✅ Archivo procesado: {len(elementos_procesados)} elementos cargados",
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
        if conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT COUNT(*) as total_archivos, 
                       COUNT(DISTINCT tipo_contenido) as tipos_contenido,
                       MAX(fecha_creacion) as ultima_actualizacion
                FROM blog_contenido
            ''')
            stats = cursor.fetchone()
            cursor.close()
            conn.close()
            
            data = [{
                "total_archivos": stats['total_archivos'],
                "tipos_contenido": stats['tipos_contenido'],
                "ultima_actualizacion": stats['ultima_actualizacion'],
                "estado": "completado"
            }]
        else:
            data = [{
                "total_archivos": 0,
                "tipos_contenido": 0,
                "ultima_actualizacion": "2024-10-30T00:00:00Z",
                "estado": "sin conexión BD"
            }]
        
        return func.HttpResponse(
            json.dumps(data),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps([{"error": str(e)}]),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )