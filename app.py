from flask import Flask, render_template, request, redirect, flash, session
import pandas as pd
import os
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import logging
import io
import openpyxl

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRETKEY', 'clave_secreta_por_defecto')

# Configuración flexible de la base de datos
def get_db_config():
    mode = os.getenv('MODE', 'local')
    if mode == 'azure':
        return {
            'host': os.getenv('DBHOST'),
            'user': os.getenv('DBUSER'),
            'password': os.getenv('DBPASSWORD'),
            'database': os.getenv('DBNAME'),
            'port': int(os.getenv('DBPORT', 3306)),
            'ssl_ca': None,
            'ssl_verify_cert': True
        }
    else:
        return {
            'host': os.getenv('DBHOST', 'localhost'),
            'user': os.getenv('DBUSER', 'root'),
            'password': os.getenv('DBPASSWORD', '1108'),
            'database': os.getenv('DBNAME', 'blog_rapido_express'),
            'port': int(os.getenv('DBPORT', 3306))
        }

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        mode = os.getenv('MODE', 'local')
        logging.info(f"Conexión exitosa a MySQL ({mode}) - Host: {config['host']}")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Error de conexión a la base de datos: {e}")
        flash(f'Error de conexión a la base de datos: {e}', 'error')
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            mode = os.getenv('MODE', 'local')
            if mode == 'local':
                cursor.execute("CREATE DATABASE IF NOT EXISTS blog_rapido_express")
                cursor.execute("USE blog_rapido_express")
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS registros_actualizacion (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    dia INT,
                    mes VARCHAR(50),
                    ano INT,
                    numero_publicacion INT,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    fecha_actualizacion DATETIME NOT NULL,
                    usuario VARCHAR(100),
                    cantidad_registros INT,
                    ip_cliente VARCHAR(45),
                    estado VARCHAR(20) DEFAULT 'completado',
                    modo_ejecucion VARCHAR(20) DEFAULT 'local'
                )
            ''')
            
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
                    fecha_creacion DATETIME,
                    FOREIGN KEY (registro_id) REFERENCES registros_actualizacion(id) ON DELETE CASCADE
                )
            ''')
            
            cursor.execute('DROP PROCEDURE IF EXISTS sp_registrar_actualizacion')
            
            cursor.execute('''
                CREATE PROCEDURE sp_registrar_actualizacion(
                    IN p_dia INT,
                    IN p_mes VARCHAR(50),
                    IN p_ano INT,
                    IN p_numero_publicacion INT,
                    IN p_nombre_archivo VARCHAR(255),
                    IN p_usuario VARCHAR(100),
                    IN p_cantidad_registros INT,
                    IN p_ip_cliente VARCHAR(45),
                    IN p_modo_ejecucion VARCHAR(20)
                )
                BEGIN
                    DECLARE EXIT HANDLER FOR SQLEXCEPTION
                    BEGIN
                        ROLLBACK;
                        RESIGNAL;
                    END;
                    
                    START TRANSACTION;
                    
                    INSERT INTO registros_actualizacion 
                    (dia, mes, ano, numero_publicacion, nombre_archivo, fecha_actualizacion, 
                     usuario, cantidad_registros, ip_cliente, estado, modo_ejecucion)
                    VALUES (p_dia, p_mes, p_ano, p_numero_publicacion, p_nombre_archivo, NOW(), 
                           p_usuario, p_cantidad_registros, p_ip_cliente, 'completado', p_modo_ejecucion);
                    
                    SELECT LAST_INSERT_ID() as registro_id;
                    
                    COMMIT;
                END
            ''')
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"Base de datos inicializada correctamente (Modo: {mode})")
            print(f"Base de datos inicializada correctamente en modo: {mode}")
            
        except mysql.connector.Error as e:
            logging.error(f"Error al inicializar la base de datos: {e}")
            flash(f'Error al inicializar la base de datos: {e}', 'error')

# ------------------ Funciones de Azure ------------------

def get_azure_blob_client():
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            return None
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client
    except Exception as e:
        logging.error(f"Error conectando a Azure Blob Storage: {e}")
        return None

def upload_to_azure_blob(file_content, filename):
    try:
        blob_service_client = get_azure_blob_client()
        if blob_service_client:
            container_client = blob_service_client.get_container_client("blog-csv-files")
            if not container_client.exists():
                container_client.create_container()
            blob_client = container_client.get_blob_client(filename)
            blob_client.upload_blob(file_content, overwrite=True)
            return True
    except Exception as e:
        logging.error(f"Error subiendo a Azure Blob: {e}")
    return False

def download_from_azure_blob(filename):
    try:
        blob_service_client = get_azure_blob_client()
        if blob_service_client:
            container_client = blob_service_client.get_container_client("blog-csv-files")
            blob_client = container_client.get_blob_client(filename)
            if blob_client.exists():
                download_stream = blob_client.download_blob()
                return download_stream.readall()
    except Exception as e:
        logging.error(f"Error descargando de Azure Blob: {e}")
    return None

# ------------------ Procesar Contenido ------------------

def procesar_contenido():
    try:
        archivo_local = 'data/contenido_blog.csv'
        azure_content = download_from_azure_blob("contenido_blog.csv")
        if azure_content:
            with open(archivo_local, 'wb') as file:
                file.write(azure_content)
            print("Archivo cargado desde Azure Blob Storage")
        
        if not os.path.exists(archivo_local):
            crear_archivo_ejemplo()
            print("Archivo de ejemplo creado automáticamente")
        
        # Leer CSV o Excel
        if archivo_local.endswith('.csv'):
            df = pd.read_csv(archivo_local)
        else:
            df = pd.read_excel(archivo_local)
        
        # Normalizar nombres de columnas
        df.rename(columns={
            'Día': 'dia',
            'Mes': 'mes',
            'Año': 'ano',
            'N° Publicación': 'numero_publicacion',
            'Tipo': 'tipo_contenido',
            'Contenido / URL': 'contenido',
            'Estilo': 'estilo'
        }, inplace=True)
        
        contenido_html = ""
        for index, fila in df.iterrows():
            tipo = str(fila['tipo_contenido']).strip().upper()
            contenido = str(fila['contenido'])
            estilo = f' style="{fila["estilo"]}"' if 'estilo' in fila and pd.notna(fila['estilo']) else ''
            
            if tipo == 'T':
                contenido_html += f'<h1{estilo}>{contenido}</h1>'
            elif tipo == 'ST':
                contenido_html += f'<h3{estilo}>{contenido}</h3>'
            elif tipo == 'P':
                contenido_html += f'<p{estilo}>{contenido}</p>'
            elif tipo == 'I':
                contenido_html += f'<img src="{contenido}"{estilo} alt="Imagen blog" class="blog-image">'
            else:
                contenido_html += f'<div{estilo}>{contenido}</div>'
        
        return contenido_html
        
    except Exception as e:
        logging.error(f"Error procesando contenido: {e}")
        return f"<p>Error al procesar el contenido: {str(e)}</p>"

# ------------------ Crear archivo de ejemplo ------------------

def crear_archivo_ejemplo():
    os.makedirs('data', exist_ok=True)
    contenido_ejemplo = '''Día,Mes,Año,N° Publicación,Tipo,Contenido / URL,Estilo
21,Octubre,2025,1,T,"Bienvenidos al Blog de Rapido Express","color:#2c3e50; font-size:36px; text-align:center;"
21,Octubre,2025,1,P,"En Rapido Express nos dedicamos a brindar el mejor servicio de mensajeria y logistica del pais. Con mas de 10 anos de experiencia, garantizamos entregas rapidas y seguras.","color:#555; font-size:18px; line-height:1.6;"'''
    with open('data/contenido_blog.csv', 'w', encoding='utf-8') as f:
        f.write(contenido_ejemplo)

# ------------------ Guardar registros en BD ------------------

def guardar_registro_actualizacion(df, nombre_archivo, usuario=None, ip_cliente=None):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Normalizar columnas
            df.rename(columns={
                'Día': 'dia',
                'Mes': 'mes',
                'Año': 'ano',
                'N° Publicación': 'numero_publicacion',
                'Tipo': 'tipo_contenido',
                'Contenido / URL': 'contenido',
                'Estilo': 'estilo'
            }, inplace=True)
            
            dia = int(df.iloc[0]['dia']) if 'dia' in df.columns and pd.notna(df.iloc[0]['dia']) else None
            mes = str(df.iloc[0]['mes']) if 'mes' in df.columns and pd.notna(df.iloc[0]['mes']) else None
            ano = int(df.iloc[0]['ano']) if 'ano' in df.columns and pd.notna(df.iloc[0]['ano']) else None
            numero_publicacion = int(df.iloc[0]['numero_publicacion']) if 'numero_publicacion' in df.columns and pd.notna(df.iloc[0]['numero_publicacion']) else None
            modo = os.getenv('MODE', 'local')
            
            cursor.callproc('sp_registrar_actualizacion', 
                          [dia, mes, ano, numero_publicacion, nombre_archivo, usuario, len(df), ip_cliente, modo])
            
            registro_id = None
            for result in cursor.stored_results():
                registro_id = result.fetchone()[0]
                break
            
            for index, fila in df.iterrows():
                cursor.execute('''
                    INSERT INTO historial_contenido 
                    (registro_id, dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo, fecha_creacion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    registro_id,
                    int(fila['dia']) if 'dia' in df.columns and pd.notna(fila['dia']) else None,
                    str(fila['mes']) if 'mes' in df.columns and pd.notna(fila['mes']) else None,
                    int(fila['ano']) if 'ano' in df.columns and pd.notna(fila['ano']) else None,
                    int(fila['numero_publicacion']) if 'numero_publicacion' in df.columns and pd.notna(fila['numero_publicacion']) else None,
                    str(fila['tipo_contenido']),
                    str(fila['contenido']),
                    str(fila['estilo']) if 'estilo' in df.columns and pd.notna(fila['estilo']) else '',
                    datetime.now()
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return registro_id
            
        except mysql.connector.Error as e:
            logging.error(f"Error al guardar registro: {e}")
            conn.rollback()
            flash(f'Error al guardar en base de datos: {e}', 'error')
            return None
    
    return None

# ------------------ Rutas Flask ------------------

@app.route('/')
def mostrar_blog():
    contenido = procesar_contenido()
    mode = os.getenv('MODE', 'local')
    return render_template('blog.html', contenido=contenido, mode=mode)

@app.route('/subir', methods=['GET', 'POST'])
def subir_archivo():
    if request.method == 'POST':
        archivo = request.files['archivo']
        usuario = request.form.get('usuario', 'Anónimo')
        
        # ✅ Obtener IP dentro del contexto de request
        ip_cliente = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
        
        if archivo and (archivo.filename.endswith('.csv') or archivo.filename.endswith('.xlsx')):
            try:
                if archivo.filename.endswith('.csv'):
                    df = pd.read_csv(archivo)
                    archivo.stream.seek(0)
                    archivo_content = archivo.read()
                else:
                    df = pd.read_excel(archivo)
                    output = io.StringIO()
                    df.to_csv(output, index=False)
                    archivo_content = output.getvalue().encode('utf-8')
                
                with open('data/contenido_blog.csv', 'wb') as f:
                    f.write(archivo_content)
                
                azure_client = get_azure_blob_client()
                if azure_client:
                    if upload_to_azure_blob(archivo_content, "contenido_blog.csv"):
                        flash('Archivo subido a Azure Blob Storage exitosamente', 'success')
                
                # ✅ Pasar ip_cliente como parámetro
                registro_id = guardar_registro_actualizacion(df, archivo.filename, usuario, ip_cliente)
                
                if registro_id:
                    mode = os.getenv('MODE', 'local')
                    flash(f'Archivo procesado exitosamente. Registro #{registro_id} guardado (Modo: {mode}).', 'success')
                else:
                    flash('Archivo procesado, pero no se pudo guardar el registro en la base de datos.', 'warning')
                    
            except Exception as e:
                flash(f'Error al procesar el archivo: {str(e)}', 'error')
                logging.error(f"Error procesando archivo: {e}")
            
            return redirect('/')
        else:
            flash('Por favor sube un archivo CSV o Excel válido', 'error')
    
    historial = obtener_historial_actualizaciones(10)
    mode = os.getenv('MODE', 'local')
    
    return render_template('subir.html', historial=historial, mode=mode)

@app.route('/historial')
def ver_historial():
    historial = obtener_historial_actualizaciones()
    mode = os.getenv('MODE', 'local')
    return render_template('historial.html', historial=historial, mode=mode)

@app.route('/detalle/<int:registro_id>')
def ver_detalle_registro(registro_id):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SELECT * FROM registros_actualizacion WHERE id = %s', (registro_id,))
            registro = cursor.fetchone()
            cursor.execute('SELECT * FROM historial_contenido WHERE registro_id = %s', (registro_id,))
            contenido = cursor.fetchall()
            cursor.close()
            conn.close()
            
            mode = os.getenv('MODE', 'local')
            return render_template('detalle.html', registro=registro, contenido=contenido, mode=mode)
            
        except mysql.connector.Error as e:
            logging.error(f"Error al obtener detalle: {e}")
            flash('Error al cargar el detalle del registro', 'error')
            return redirect('/historial')
    
    return redirect('/historial')

@app.route('/info')
def info_sistema():
    mode = os.getenv('MODE', 'local')
    db_config = get_db_config()
    
    info = {
        'modo': mode,
        'db_host': db_config['host'],
        'db_name': db_config['database'],
        'db_user': db_config['user'],
        'azure_configured': bool(get_azure_blob_client())
    }
    
    return render_template('info.html', info=info)

def obtener_historial_actualizaciones(limit=50):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT id, dia, mes, ano, numero_publicacion, nombre_archivo, 
                       fecha_actualizacion, usuario, cantidad_registros, ip_cliente, estado, modo_ejecucion
                FROM registros_actualizacion 
                ORDER BY fecha_actualizacion DESC 
                LIMIT %s
            ''', (limit,))
            historial = cursor.fetchall()
            cursor.close()
            conn.close()
            return historial
        except mysql.connector.Error as e:
            logging.error(f"Error al obtener historial: {e}")
            return []
    return []

# ------------------ Inicializar DB ------------------

with app.app_context():
    init_db()

if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    mode = os.getenv('MODE', 'local')
    print("Iniciando Blog Rapido Express...")
    print(f"Modo: {mode}")
    print(f"Base de datos: {get_db_config()['host']}")
    print("Servidor: http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)