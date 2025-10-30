import azure.functions as func
import json
import mysql.connector
import os
import logging
import csv
import io
from datetime import datetime

# Configuración DIRECTA - sin variables de entorno
def get_db_config():
    return {
        'host': 'blog-rapido-express-mysql.mysql.database.azure.com',
        'user': 'admin_blog',
        'password': 'PasswordSeguro123!',
        'database': 'blog_rapido_express',
        'port': 3306,
        'ssl_ca': './DigiCertGlobalRootG2.crt.pem',
        'ssl_verify_cert': False,  # IMPORTANTE: desactivar verificación SSL
        'charset': 'utf8mb4',
        'connect_timeout': 30
    }

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        logging.info("✅ CONEXIÓN EXITOSA A MYSQL")
        return conn
    except Exception as e:
        logging.error(f"❌ ERROR DE CONEXIÓN: {str(e)}")
        return None

# Crear certificado SSL si no existe
cert_content = """-----BEGIN CERTIFICATE-----
MIIDjjCCAnagAwIBAgIQAzrx5qcRqaC7KGSxHQn65TANBgkqhkiG9w0BAQsFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRigl2aUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBH
MjAeFw0xMzA4MDExMjAwMDBaFw0zODAxMTUxMjAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IEcyMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuzfNNNx7a8myaJCtSnX/RrohCgiN9RlUyfuI
2/Ou8jqJkTx65qsGGmvPrC3oXgkkRLpimn7Wo6h+4FR1IAWsULecYxpsMNzaHxmx
1x7e/dfgy5SDN67sH0NO3Xss0r0upS/kqbitOtSZpLYl6ZtrAGCSYP9PIUkY92eQ
q2EGnI/yuum06ZIya7XzV+hdG82MHauVBJVJ8zUtluNJbd134/tJS7SsVQepj5Wz
tCO7TG1F8PapspUwtP1MVYwnSlcUfIKdzXOS0xZKBgyMUNGPHgm+F6HmIcr9g+UQ
vIOlCsRnKPZzFBQ9RnbDhxSJITRNrw9FDKZJobq7nMWxM4MphQIDAQABo0IwQDAP
BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBhjAdBgNVHQ4EFgQUTiJUIBiV
5uNu5g/6+rkS7QYXjzkwDQYJKoZIhvcNAQELBQADggEBAGBnKJRvDkhj6zHd6mcY
1Yl9PMWLSn/pvtsrF9+wX3N3KjITOYFnQoQj8kVnNeyIv/iPsGEMNKSuIEyExtv4
NeF22d+mQrvHRAiGfzZ0JFrabA0UWTW98kndth/Jsw1HKj2ZL7tcu7XUIOGZX1NG
Fdtom/DzMNU+MeKNhJ7jitralj41E6Vf8PlwUHBHQRFXGU7Aj64GxJUTFy8bJZ91
8rGOmaFvE7FBcf6IKshPECBV1/MUReXgRPTqh5Uykw7+U0b6LJ3/iyK5S9kJRaTe
pLiaWN0bfVKfjllDiIGknibVb63dDcY3fe0Dkhvld1927jyNxF1WW6LZZm6zNTfl
MrY=
-----END CERTIFICATE-----"""

# Crear archivo de certificado
with open('./DigiCertGlobalRootG2.crt.pem', 'w') as f:
    f.write(cert_content)

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
                conn.close()
            except Exception as e:
                logging.error(f"Error contando elementos: {str(e)}")
        
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

# Endpoint simple para probar conexión
@app.route(route="test", methods=["GET"])
def test_connection(req: func.HttpRequest) -> func.HttpResponse:
    conn = get_db_connection()
    if conn:
        conn.close()
        return func.HttpResponse(
            json.dumps({"status": "success", "message": "✅ Conexión a BD exitosa"}),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
    else:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "❌ No se pudo conectar a BD"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.route(route="subir", methods=["POST", "OPTIONS"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    # Manejar preflight CORS
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
        # Verificar si hay archivos
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
        
        # Verificar que sea CSV
        if not file.filename.lower().endswith('.csv'):
            return func.HttpResponse(
                json.dumps({"error": "Solo se permiten archivos CSV"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        # Leer y procesar archivo
        file_content = file.read()
        
        # Procesar CSV manualmente (versión simplificada)
        try:
            content_str = file_content.decode('utf-8')
            reader = csv.DictReader(io.StringIO(content_str))
            
            elementos_procesados = []
            for row in reader:
                # Procesar cada fila con valores por defecto
                try:
                    dia = int(row['Día']) if row['Día'].strip() else 1
                except:
                    dia = 1
                    
                mes = row['Mes'] if row['Mes'].strip() else 'Enero'
                
                try:
                    ano = int(row['Año']) if row['Año'].strip() else 2024
                except:
                    ano = 2024
                    
                try:
                    numero_publicacion = int(row['N° Publicación']) if row['N° Publicación'].strip() else 1
                except:
                    numero_publicacion = 1
                    
                tipo_contenido = row['Tipo'].strip().upper()
                contenido = row['Contenido / URL'].strip()
                estilo = row.get('Estilo', '').strip()
                
                elemento = {
                    'dia': dia,
                    'mes': mes,
                    'ano': ano,
                    'numero_publicacion': numero_publicacion,
                    'tipo_contenido': tipo_contenido,
                    'contenido': contenido,
                    'estilo': estilo
                }
                elementos_procesados.append(elemento)
            
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"error": f"Error procesando CSV: {str(e)}"}),
                status_code=400,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
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
            
            # INSERT CORREGIDO con fecha explícita
            cursor.execute('''
                INSERT INTO registros_actualizacion 
                (nombre_archivo, usuario, cantidad_registros, fecha_actualizacion)
                VALUES (%s, %s, %s, NOW())
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
            logging.error(f"Error en transacción BD: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": f"Error al guardar en base de datos: {str(e)}"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                
    except Exception as e:
        logging.error(f"Error en /subir: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error del servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )

@app.route(route="init-tables", methods=["GET"])
def init_tables(req: func.HttpRequest) -> func.HttpResponse:
    """Endpoint para inicializar tablas manualmente - VERSIÓN CORREGIDA"""
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "No hay conexión a BD"}),
                status_code=500,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        cursor = conn.cursor()
        
        # ELIMINAR tablas existentes si hay problemas
        cursor.execute("DROP TABLE IF EXISTS blog_contenido")
        cursor.execute("DROP TABLE IF EXISTS registros_actualizacion")
        
        # Tabla de registros - VERSIÓN CORREGIDA
        cursor.execute('''
            CREATE TABLE registros_actualizacion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre_archivo VARCHAR(255) NOT NULL,
                usuario VARCHAR(100) DEFAULT 'Anonimo',
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                cantidad_registros INT DEFAULT 0
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # Tabla de contenido - VERSIÓN CORREGIDA
        cursor.execute('''
            CREATE TABLE blog_contenido (
                id INT AUTO_INCREMENT PRIMARY KEY,
                registro_id INT,
                dia INT,
                mes VARCHAR(50),
                ano INT,
                numero_publicacion INT,
                tipo_contenido VARCHAR(10),
                contenido TEXT,
                estilo TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps({
                "success": True, 
                "message": "✅ Tablas recreadas exitosamente con valores por defecto correctos"
            }),
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