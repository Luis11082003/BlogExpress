import azure.functions as func
import json

app = func.FunctionApp()

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"status": "healthy", "message": "API Working"}),
        status_code=200,
        mimetype="application/json"
    )

@app.route(route="blog", methods=["GET"])
def get_blog(req: func.HttpRequest) -> func.HttpResponse:
    data = [
        {
            "dia": 30,
            "mes": "Octubre", 
            "ano": 2024,
            "tipo_contenido": "T",
            "contenido": "🚀 BLOG EXPRESS - FUNCIONANDO",
            "estilo": "color: blue;"
        }
    ]
    return func.HttpResponse(
        json.dumps(data),
        status_code=200,
        mimetype="application/json"
    )

@app.route(route="subir", methods=["POST"])
def subir_archivo(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"success": True, "message": "File uploaded"}),
        status_code=200,
        mimetype="application/json"
    )