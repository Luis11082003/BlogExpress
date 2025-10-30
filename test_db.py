import mysql.connector
from mysql.connector.constants import ClientFlag
import os

# Ruta absoluta al certificado descargado
ssl_cert_path = os.path.expanduser("~/DigiCertGlobalRootCA.crt.pem")

config = {
    "host": "blog-rapido-express-mysql.mysql.database.azure.com",
    "user": "admin_blog@blog-rapido-express-mysql",
    "password": "PasswordSeguro123!",
    "database": "blog_rapido_express",
    "port": 3306,
    "client_flags": [ClientFlag.SSL],
    "ssl_ca": ssl_cert_path,
}

try:
    conn = mysql.connector.connect(**config)
    print("✅ CONEXIÓN EXITOSA!")

    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    print("📊 Tablas en la base de datos:")
    for table in tables:
        print(f" - {table[0]}")

    cursor.close()
    conn.close()

except mysql.connector.Error as e:
    print(f"❌ ERROR MySQL: {e}")
except Exception as e:
    print(f"❌ ERROR: {e}")
