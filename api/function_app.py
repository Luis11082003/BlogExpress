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