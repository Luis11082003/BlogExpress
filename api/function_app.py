import azure.functions as func
import json
import mysql.connector
import os
import logging

# Configuración de base de datos - CON TUS VARIABLES REALES
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
        json.dumps({"status": "healthy", "message": "Sistema completo con MySQL"}),
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
                "tipo_contenido": "P", "contenido": "Usa la página 'Subir' para cargar un archivo CSV y ver el contenido aquí.", 
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
        # Simular procesamiento de archivo
        import pandas as pd
        import io
        
        if not req.files:
            return func.HttpResponse(
                json.dumps({"error": "No se recibió archivo"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if file and file.filename.lower().endswith('.csv'):
            file_content = file.read()
            
            # Procesar CSV
            df = pd.read_csv(io.BytesIO(file_content))
            elementos_procesados = []
            
            for index, row in df.iterrows():
                elemento = {
                    'dia': int(row['Día']) if 'Día' in df.columns and pd.notna(row['Día']) else 30,
                    'mes': str(row['Mes']) if 'Mes' in df.columns and pd.notna(row['Mes']) else 'Octubre',
                    'ano': int(row['Año']) if 'Año' in df.columns and pd.notna(row['Año']) else 2024,
                    'numero_publicacion': int(row['N° Publicación']) if 'N° Publicación' in df.columns and pd.notna(row['N° Publicación']) else 1,
                    'tipo_contenido': str(row['Tipo']).strip().upper() if 'Tipo' in df.columns and pd.notna(row['Tipo']) else 'P',
                    'contenido': str(row['Contenido / URL']) if 'Contenido / URL' in df.columns and pd.notna(row['Contenido / URL']) else 'Contenido',
                    'estilo': str(row['Estilo']) if 'Estilo' in df.columns and pd.notna(row['Estilo']) else ''
                }
                elementos_procesados.append(elemento)
            
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
                    "message": f"✅ Archivo procesado: {len(elementos_procesados)} elementos",
                    "elementos_procesados": len(elementos_procesados),
                    "tipos_contenido": tipos
                }),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Archivo CSV requerido"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Error: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.route(route="historial", methods=["GET"])
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    data = [
        {
            "id": 1,
            "nombre_archivo": "demo.csv", 
            "usuario": "Admin",
            "fecha_actualizacion": "2024-10-30T00:00:00Z",
            "cantidad_registros": 3,
            "estado": "completado"
        }
    ]
    return func.HttpResponse(
        json.dumps(data),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )