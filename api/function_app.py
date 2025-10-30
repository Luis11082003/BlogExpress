import azure.functions as func
import json
import pandas as pd
import mysql.connector
from datetime import datetime
import os
import io
import openpyxl

# Configuración de base de datos - usa variables de entorno de Azure
def get_db_config():
    return {
        'host': os.environ.get('DBHOST'),
        'user': os.environ.get('DBUSER'),
        'password': os.environ.get('DBPASSWORD'),
        'database': os.environ.get('DBNAME'),
        'port': int(os.environ.get('DBPORT', '3306')),
        'ssl_ca': None,
        'ssl_verify_cert': False,  # Cambiado a False para Azure MySQL
        'ssl_disabled': True  # Agregar esta línea
    }

def get_db_connection():
    try:
        config = get_db_config()
        conn = mysql.connector.connect(**config)
        print(f"✅ Conectado a MySQL: {config['host']}")
        return conn
    except Exception as e:
        print(f"❌ Error de conexión a BD: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Tabla de registros de actualización
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
                    modo_ejecucion VARCHAR(20) DEFAULT 'azure'
                )
            ''')
            
            # Tabla de contenido del blog
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
            
            # Procedimiento almacenado para transacciones
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
                    IN p_ip_cliente VARCHAR(45)
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
                           p_usuario, p_cantidad_registros, p_ip_cliente, 'completado', 'azure');
                    
                    SELECT LAST_INSERT_ID() as registro_id;
                    
                    COMMIT;
                END
            ''')
            
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Base de datos inicializada correctamente")
            
        except Exception as e:
            print(f"❌ Error inicializando BD: {e}")

# Procesar archivo según requerimientos de la práctica
def procesar_archivo(file_content, filename):
    try:
        # Determinar tipo de archivo
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
        elif filename.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            return None, "Formato no soportado. Use CSV o Excel"
        
        # Normalizar nombres de columnas (case insensitive)
        column_mapping = {
            'día': 'dia', 'dia': 'dia', 'day': 'dia',
            'mes': 'mes', 'month': 'mes',
            'año': 'ano', 'ano': 'ano', 'aÃ±o': 'ano', 'year': 'ano',
            'n° publicación': 'numero_publicacion', 'numero_publicacion': 'numero_publicacion', 
            'n publicación': 'numero_publicacion', 'publicacion': 'numero_publicacion',
            'tipo': 'tipo_contenido', 'tipo contenido': 'tipo_contenido',
            'contenido / url': 'contenido', 'contenido': 'contenido', 'contenido url': 'contenido',
            'estilo': 'estilo', 'css': 'estilo'
        }
        
        df.columns = [column_mapping.get(col.strip().lower(), col.strip().lower()) for col in df.columns]
        
        # Verificar columnas requeridas
        columnas_requeridas = ['dia', 'mes', 'ano', 'numero_publicacion', 'tipo_contenido', 'contenido']
        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
        
        if columnas_faltantes:
            return None, f"Columnas requeridas faltantes: {', '.join(columnas_faltantes)}"
        
        # Procesar cada fila según requerimientos
        contenido_procesado = []
        for index, row in df.iterrows():
            # Obtener y validar datos
            tipo = str(row['tipo_contenido']).strip().upper()
            contenido = str(row['contenido']).strip()
            estilo = str(row.get('estilo', '')).strip()
            
            # Validar tipo según práctica (T, ST, P, I)
            if tipo not in ['T', 'ST', 'P', 'I']:
                return None, f"Tipo '{tipo}' no válido en fila {index+1}. Use: T (Título), ST (Subtítulo), P (Párrafo), I (Imagen)"
            
            # Validar contenido según tipo
            if tipo == 'I' and not (contenido.startswith('http://') or contenido.startswith('https://')):
                return None, f"Fila {index+1}: Las imágenes (Tipo I) deben contener una URL válida"
            
            # Convertir datos numéricos
            try:
                dia = int(row['dia']) if pd.notna(row['dia']) else None
                ano = int(row['ano']) if pd.notna(row['ano']) else None
                numero_publicacion = int(row['numero_publicacion']) if pd.notna(row['numero_publicacion']) else None
                mes = str(row['mes']).strip() if pd.notna(row['mes']) else ''
            except (ValueError, TypeError):
                return None, f"Fila {index+1}: Error en datos numéricos (Día, Año o N° Publicación)"
            
            contenido_procesado.append({
                'dia': dia,
                'mes': mes,
                'ano': ano,
                'numero_publicacion': numero_publicacion,
                'tipo_contenido': tipo,
                'contenido': contenido,
                'estilo': estilo,
                'fila': index + 1
            })
        
        return contenido_procesado, None
        
    except Exception as e:
        return None, f"Error procesando archivo: {str(e)}"

# Azure Functions App
app = func.FunctionApp()

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener contenido del blog para mostrar en la página principal"""
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a la base de datos"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT h.dia, h.mes, h.ano, h.numero_publicacion, 
                   h.tipo_contenido, h.contenido, h.estilo
            FROM historial_contenido h
            INNER JOIN registros_actualizacion r ON h.registro_id = r.id
            ORDER BY r.fecha_actualizacion DESC, h.fecha_creacion ASC
            LIMIT 100
        ''')
        contenido = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps(contenido),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="subir", methods=["POST"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    """Subir archivo CSV/Excel - REQUERIMIENTO PRINCIPAL DE LA PRÁCTICA"""
    try:
        # Verificar si hay archivos en la solicitud
        if not req.files:
            return func.HttpResponse(
                json.dumps({"error": "No se recibió ningún archivo. Por favor seleccione un archivo CSV o Excel."}),
                status_code=400,
                mimetype="application/json"
            )
        
        file = req.files.get('archivo')
        usuario = req.form.get('usuario', 'Anónimo')
        
        if not file or not file.filename:
            return func.HttpResponse(
                json.dumps({"error": "No se proporcionó un archivo válido"}),
                status_code=400,
                mimetype="application/json"
            )
        
        print(f"📤 Procesando archivo: {file.filename}")
        
        # Leer y procesar archivo
        file_content = file.read()
        contenido_procesado, error = procesar_archivo(file_content, file.filename)
        
        if error:
            return func.HttpResponse(
                json.dumps({"error": error}),
                status_code=400,
                mimetype="application/json"
            )
        
        print(f"✅ Archivo procesado: {len(contenido_procesado)} filas válidas")
        
        # Guardar en base de datos con transacción
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a la base de datos"}),
                status_code=500,
                mimetype="application/json"
            )
        
        try:
            cursor = conn.cursor()
            
            # Usar procedimiento almacenado para registrar la actualización
            primer_registro = contenido_procesado[0]
            cursor.callproc('sp_registrar_actualizacion', [
                primer_registro['dia'],
                primer_registro['mes'],
                primer_registro['ano'],
                primer_registro['numero_publicacion'],
                file.filename,
                usuario,
                len(contenido_procesado),
                'Azure-Static-Web-App'
            ])
            
            # Obtener ID del registro
            registro_id = None
            for result in cursor.stored_results():
                registro_id = result.fetchone()[0]
                break
            
            print(f"📝 Registro creado: ID {registro_id}")
            
            # Insertar contenido en historial
            for item in contenido_procesado:
                cursor.execute('''
                    INSERT INTO historial_contenido 
                    (registro_id, dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo, fecha_creacion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    registro_id,
                    item['dia'],
                    item['mes'],
                    item['ano'],
                    item['numero_publicacion'],
                    item['tipo_contenido'],
                    item['contenido'],
                    item['estilo'],
                    datetime.now()
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"🎉 Archivo guardado exitosamente. Registro #{registro_id}")
            
            return func.HttpResponse(
                json.dumps({
                    "success": True, 
                    "registro_id": registro_id,
                    "mensaje": f"✅ Archivo procesado exitosamente. {len(contenido_procesado)} elementos cargados.",
                    "elementos_procesados": len(contenido_procesado),
                    "tipos_contenido": {
                        'T': len([x for x in contenido_procesado if x['tipo_contenido'] == 'T']),
                        'ST': len([x for x in contenido_procesado if x['tipo_contenido'] == 'ST']),
                        'P': len([x for x in contenido_procesado if x['tipo_contenido'] == 'P']),
                        'I': len([x for x in contenido_procesado if x['tipo_contenido'] == 'I'])
                    }
                }),
                status_code=200,
                mimetype="application/json"
            )
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error en transacción: {e}")
            raise e
            
    except Exception as e:
        print(f"❌ Error general: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Error en el servidor: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="historial", methods=["GET"])
def get_historial(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener historial de actualizaciones"""
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a la base de datos"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT id, dia, mes, ano, numero_publicacion, nombre_archivo, 
                   fecha_actualizacion, usuario, cantidad_registros, estado
            FROM registros_actualizacion 
            ORDER BY fecha_actualizacion DESC 
            LIMIT 50
        ''')
        historial = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps(historial),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="detalle/{id}", methods=["GET"])
def get_detalle(req: func.HttpRequest) -> func.HttpResponse:
    """Obtener detalle de un registro específico"""
    try:
        registro_id = req.route_params.get('id')
        
        if not registro_id or not registro_id.isdigit():
            return func.HttpResponse(
                json.dumps({"error": "ID de registro inválido"}),
                status_code=400,
                mimetype="application/json"
            )
        
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse(
                json.dumps({"error": "Error de conexión a la base de datos"}),
                status_code=500,
                mimetype="application/json"
            )
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM registros_actualizacion WHERE id = %s', (registro_id,))
        registro = cursor.fetchone()
        
        if not registro:
            cursor.close()
            conn.close()
            return func.HttpResponse(
                json.dumps({"error": "Registro no encontrado"}),
                status_code=404,
                mimetype="application/json"
            )
        
        cursor.execute('''
            SELECT dia, mes, ano, numero_publicacion, tipo_contenido, contenido, estilo
            FROM historial_contenido 
            WHERE registro_id = %s 
            ORDER BY fecha_creacion ASC
        ''', (registro_id,))
        contenido = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return func.HttpResponse(
            json.dumps({
                "registro": registro, 
                "contenido": contenido,
                "total_elementos": len(contenido)
            }),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Endpoint de salud del sistema"""
    try:
        conn = get_db_connection()
        db_status = "connected" if conn else "disconnected"
        db_info = {}
        
        if conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SELECT COUNT(*) as total FROM historial_contenido')
            db_info['total_elementos'] = cursor.fetchone()['total']
            cursor.execute('SELECT COUNT(*) as total FROM registros_actualizacion')
            db_info['total_registros'] = cursor.fetchone()['total']
            cursor.close()
            conn.close()
        
        return func.HttpResponse(
            json.dumps({
                "status": "healthy", 
                "mode": "azure",
                "database": db_status,
                "database_info": db_info,
                "timestamp": datetime.now().isoformat(),
                "version": "2.0"
            }),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# Inicializar base de datos al cargar la función
print("🚀 Inicializando Sistema de Blog Express...")
print("📊 Configurando base de datos...")
init_db()
print("✅ Sistema listo para recibir solicitudes")