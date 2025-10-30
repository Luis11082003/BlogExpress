// API Base URL
const API_BASE = '/api';

// Función para cargar contenido del blog
async function cargarBlog() {
    try {
        const response = await fetch(`${API_BASE}/blog`);
        const contenido = await response.json();
        
        if (response.ok) {
            mostrarContenidoBlog(contenido);
        } else {
            document.getElementById('contenido-blog').innerHTML = 
                '<p>Error al cargar el contenido del blog</p>';
        }
    } catch (error) {
        document.getElementById('contenido-blog').innerHTML = 
            '<p>Error de conexión: ' + error.message + '</p>';
    }
}

// Función para mostrar contenido del blog
function mostrarContenidoBlog(contenido) {
    const contenedor = document.getElementById('contenido-blog');
    let html = '';
    
    contenido.forEach(item => {
        const estilo = item.estilo || '';
        
        switch(item.tipo_contenido?.toUpperCase()) {
            case 'T':
                html += `<h1 style="${estilo}">${item.contenido}</h1>`;
                break;
            case 'ST':
                html += `<h3 style="${estilo}">${item.contenido}</h3>`;
                break;
            case 'P':
                html += `<p style="${estilo}">${item.contenido}</p>`;
                break;
            case 'I':
                html += `<img src="${item.contenido}" style="${estilo}" alt="Imagen blog" class="blog-image">`;
                break;
            default:
                html += `<div style="${estilo}">${item.contenido}</div>`;
        }
    });
    
    contenedor.innerHTML = html || '<p>No hay contenido disponible</p>';
}

// Función para subir archivo
async function subirArchivo(event) {
    event.preventDefault();
    
    const formData = new FormData(event.target);
    const boton = event.target.querySelector('button[type="submit"]');
    const estado = document.getElementById('estado-subida');
    
    try {
        boton.disabled = true;
        estado.textContent = 'Subiendo archivo...';
        estado.className = 'estado-cargando';
        
        const response = await fetch(`${API_BASE}/subir`, {
            method: 'POST',
            body: formData
        });
        
        const resultado = await response.json();
        
        if (response.ok) {
            estado.textContent = `Archivo subido exitosamente. Registro #${resultado.registro_id}`;
            estado.className = 'estado-exito';
            event.target.reset();
            
            // Recargar el blog después de subir
            setTimeout(() => cargarBlog(), 1000);
        } else {
            estado.textContent = `Error: ${resultado.error}`;
            estado.className = 'estado-error';
        }
    } catch (error) {
        estado.textContent = `Error de conexión: ${error.message}`;
        estado.className = 'estado-error';
    } finally {
        boton.disabled = false;
    }
}

// Función para cargar historial
async function cargarHistorial() {
    try {
        const response = await fetch(`${API_BASE}/historial`);
        const historial = await response.json();
        
        if (response.ok) {
            mostrarHistorial(historial);
        } else {
            document.getElementById('contenido-historial').innerHTML = 
                '<p>Error al cargar el historial</p>';
        }
    } catch (error) {
        document.getElementById('contenido-historial').innerHTML = 
            '<p>Error de conexión: ' + error.message + '</p>';
    }
}

// Función para mostrar historial
function mostrarHistorial(historial) {
    const contenedor = document.getElementById('contenido-historial');
    
    if (historial.length === 0) {
        contenedor.innerHTML = '<p>No hay registros en el historial</p>';
        return;
    }
    
    let html = `
        <table class="tabla-historial">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Archivo</th>
                    <th>Usuario</th>
                    <th>Fecha</th>
                    <th>Registros</th>
                    <th>Acciones</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    historial.forEach(item => {
        const fecha = new Date(item.fecha_actualizacion).toLocaleString();
        html += `
            <tr>
                <td>${item.id}</td>
                <td>${item.nombre_archivo}</td>
                <td>${item.usuario || 'Anónimo'}</td>
                <td>${fecha}</td>
                <td>${item.cantidad_registros}</td>
                <td>
                    <a href="/detalle.html?id=${item.id}" class="btn-ver">Ver Detalle</a>
                </td>
            </tr>
        `;
    });
    
    html += '</tbody></table>';
    contenedor.innerHTML = html;
}

// Función para cargar detalle
async function cargarDetalle() {
    const urlParams = new URLSearchParams(window.location.search);
    const registroId = urlParams.get('id');
    
    if (!registroId) {
        document.getElementById('contenido-detalle').innerHTML = '<p>ID de registro no especificado</p>';
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/detalle/${registroId}`);
        const data = await response.json();
        
        if (response.ok) {
            mostrarDetalle(data.registro, data.contenido);
        } else {
            document.getElementById('contenido-detalle').innerHTML = 
                '<p>Error al cargar el detalle</p>';
        }
    } catch (error) {
        document.getElementById('contenido-detalle').innerHTML = 
            '<p>Error de conexión: ' + error.message + '</p>';
    }
}

// Función para mostrar detalle
function mostrarDetalle(registro, contenido) {
    const contenedor = document.getElementById('contenido-detalle');
    
    let html = `
        <div class="detalle-registro">
            <h2>Detalle del Registro #${registro.id}</h2>
            <div class="info-registro">
                <p><strong>Archivo:</strong> ${registro.nombre_archivo}</p>
                <p><strong>Usuario:</strong> ${registro.usuario || 'Anónimo'}</p>
                <p><strong>Fecha:</strong> ${new Date(registro.fecha_actualizacion).toLocaleString()}</p>
                <p><strong>Registros:</strong> ${registro.cantidad_registros}</p>
            </div>
            <h3>Contenido:</h3>
            <div class="contenido-detalle">
    `;
    
    contenido.forEach(item => {
        const estilo = item.estilo || '';
        
        switch(item.tipo_contenido?.toUpperCase()) {
            case 'T':
                html += `<h4 style="${estilo}">${item.contenido}</h4>`;
                break;
            case 'ST':
                html += `<h5 style="${estilo}">${item.contenido}</h5>`;
                break;
            case 'P':
                html += `<p style="${estilo}">${item.contenido}</p>`;
                break;
            case 'I':
                html += `<img src="${item.contenido}" style="${estilo}" alt="Imagen" class="imagen-detalle">`;
                break;
            default:
                html += `<div style="${estilo}">${item.contenido}</div>`;
        }
    });
    
    html += '</div></div>';
    contenedor.innerHTML = html;
}

// Función para mostrar información del sistema
function cargarInfoSistema() {
    const info = {
        modo: 'azure',
        db_host: 'blog-rapido-express-mysql.mysql.database.azure.com',
        azure_configured: true
    };
    
    const contenedor = document.getElementById('info-sistema');
    contenedor.innerHTML = `
        <div class="info-card">
            <h3>Información del Sistema</h3>
            <p><strong>Modo:</strong> ${info.modo}</p>
            <p><strong>Servidor BD:</strong> ${info.db_host}</p>
            <p><strong>Azure Configurado:</strong> ${info.azure_configured ? 'Sí' : 'No'}</p>
            <p><strong>Frontend:</strong> HTML/JavaScript</p>
            <p><strong>Backend:</strong> Azure Functions</p>
        </div>
    `;
}

// Utilidad para mostrar mensajes
function mostrarMensaje(mensaje, tipo = 'info') {
    const mensajeDiv = document.createElement('div');
    mensajeDiv.className = `mensaje mensaje-${tipo}`;
    mensajeDiv.textContent = mensaje;
    
    document.body.appendChild(mensajeDiv);
    
    setTimeout(() => {
        mensajeDiv.remove();
    }, 5000);
}