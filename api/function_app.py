import azure.functions as func
import json

app = func.FunctionApp()

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"status": "healthy", "message": "API Working"}),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    data = [
        {
            "dia": 30,
            "mes": "Octubre", 
            "ano": 2024,
            "numero_publicacion": 1,
            "tipo_contenido": "T",
            "contenido": "🚀 BLOG EXPRESS - SISTEMA COMPLETO",
            "estilo": "color:#2c3e50; text-align:center"
        },
        {
            "dia": 30,
            "mes": "Octubre",
            "ano": 2024, 
            "numero_publicacion": 1,
            "tipo_contenido": "P",
            "contenido": "¡El sistema está funcionando correctamente! Puedes subir archivos CSV para generar contenido dinámico.",
            "estilo": "color:#555; background:#f8f9fa; padding:1rem; border-radius:8px"
        },
        {
            "dia": 30,
            "mes": "Octubre",
            "ano": 2024, 
            "numero_publicacion": 1,
            "tipo_contenido": "ST",
            "contenido": "Práctica 2 - Generación Dinámica de Contenido",
            "estilo": "color:#3498db; text-align:center"
        }
    ]
    return func.HttpResponse(
        json.dumps(data),
        status_code=200,
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
    
    # Procesar subida (versión simple por ahora)
    return func.HttpResponse(
        json.dumps({
            "success": True, 
            "message": "✅ Archivo recibido correctamente",
            "elementos_procesados": 3,
            "registro_id": 1,
            "tipos_contenido": {
                "T": 1,
                "ST": 1, 
                "P": 1
            }
        }),
        status_code=200,
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
