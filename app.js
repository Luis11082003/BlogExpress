// API Base URL
const API_BASE = '/api';

// Función para cargar contenido del blog - REQUERIMIENTO PRINCIPAL
async function cargarBlog() {
    try {
        mostrarCargandoBlog();
        
        const response = await fetch(`${API_BASE}/blog`);
        const contenido = await response.json();
        
        if (response.ok) {
            mostrarContenidoBlog(contenido);
        } else {
            mostrarErrorBlog('Error al cargar el contenido del blog: ' + (contenido.error || 'Error desconocido'));
        }
    } catch (error) {
        mostrarErrorBlog('Error de conexión: ' + error.message);
    }
}

function mostrarCargandoBlog() {
    const contenedor = document.getElementById('contenido-blog');
    contenedor.innerHTML = `
        <div style="text-align: center; padding: 3rem;">
            <div style="font-size: 48px; margin-bottom: 1rem;">📊</div>
            <h3 style="color: #3498db;">Cargando contenido del blog...</h3>
            <p style="color: #7f8c8d;">Generando contenido dinámicamente desde la base de datos</p>
        </div>
    `;
}

// Función para mostrar contenido del blog según requerimientos de la práctica
function mostrarContenidoBlog(contenido) {
    const contenedor = document.getElementById('contenido-blog');
    
    if (!contenido || contenido.length === 0) {
        contenedor.innerHTML = `
            <div class="blog-vacio">
                <h1 style="color: #2c3e50; text-align: center; margin-bottom: 1rem;">🚀 Blog de Rápido Express</h1>
                <p style="text-align: center; color: #7f8c8d; font-size: 18px; margin-bottom: 2rem;">
                    Sistema de Generación Dinámica de Contenido
                </p>
                
                <div style="background: #f8f9fa; padding: 2rem; border-radius: 8px; margin: 2rem 0;">
                    <h3 style="color: #2c3e50; text-align: center;">📝 No hay contenido disponible</h3>
                    <p style="text-align: center; color: #95a5a6;">
                        El blog está listo para recibir contenido. Sube un archivo CSV o Excel con el formato requerido.
                    </p>
                </div>
                
                <div style="text-align: center; margin-top: 2rem;">
                    <a href="subir.html" class="btn-subir">📤 Subir Primer Archivo</a>
                </div>
                
                <div style="margin-top: 3rem; padding: 2rem; background: white; border-radius: 8px; border-left: 4px solid #3498db;">
                    <h3 style="color: #2c3e50; margin-bottom: 1rem;">📋 Formato Requerido del Archivo</h3>
                    
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-bottom: 2rem;">
                        <div>
                            <h4 style="color: #34495e;">Columnas Obligatorias:</h4>
                            <ul style="color: #555;">
                                <li><strong>Día</strong> (número del día)</li>
                                <li><strong>Mes</strong> (nombre del mes)</li>
                                <li><strong>Año</strong> (número del año)</li>
                                <li><strong>N° Publicación</strong> (número)</li>
                                <li><strong>Tipo</strong> (T, ST, P, I)</li>
                                <li><strong>Contenido / URL</strong> (texto o enlace)</li>
                            </ul>
                        </div>
                        
                        <div>
                            <h4 style="color: #34495e;">Tipos de Contenido:</h4>
                            <ul style="color: #555;">
                                <li><strong>T</strong> → Título principal (&lt;h1&gt;)</li>
                                <li><strong>ST</strong> → Subtítulo (&lt;h3&gt;)</li>
                                <li><strong>P</strong> → Párrafo (&lt;p&gt;)</li>
                                <li><strong>I</strong> → Imagen (&lt;img&gt;)</li>
                            </ul>
                        </div>
                    </div>
                    
                    <div style="background: #f8f9fa; padding: 1rem; border-radius: 4px;">
                        <h5 style="color: #2c3e50; margin-bottom: 0.5rem;">Ejemplo de CSV:</h5>
                        <pre style="background: white; padding: 1rem; border-radius: 4px; overflow-x: auto; font-size: 12px;">
Día,Mes,Año,N° Publicación,Tipo,Contenido / URL,Estilo
21,Octubre,2025,1,T,"Bienvenidos al Blog","color:#2c3e50; text-align:center"
21,Octubre,2025,1,P,"Texto del párrafo...","color:#555; font-size:16px"</pre>
                    </div>
                </div>
            </div>
        `;
        return;
    }
    
    let html = '';
    let publicacionActual = null;
    let contadorPublicaciones = 0;
    
    contenido.forEach((item, index) => {
        const estilo = item.estilo || '';
        
        // Agrupar por publicación
        if (item.numero_publicacion !== publicacionActual) {
            if (publicacionActual !== null) {
                html += `<hr style="margin: 3rem 0; border: none; border-top: 2px dashed #ecf0f1;">`;
            }
            publicacionActual = item.numero_publicacion;
            contadorPublicaciones++;
            
            // Mostrar información de la publicación
            if (item.dia && item.mes && item.ano) {
                html += `
                    <div style="text-align: center; margin-bottom: 2rem; padding: 1rem; background: #f8f9fa; border-radius: 8px;">
                        <small style="color: #7f8c8d; font-size: 14px;">
                            📅 Publicación #${item.numero_publicacion} - ${item.dia} de ${item.mes} de ${item.ano}
                        </small>
                    </div>
                `;
            }
        }
        
        // Generar contenido según tipo - REQUERIMIENTO PRINCIPAL
        switch(item.tipo_contenido?.toUpperCase()) {
            case 'T': // Título principal <h1>
                html += `<h1 style="${estilo}">${item.contenido}</h1>`;
                break;
                
            case 'ST': // Subtítulo <h3>
                html += `<h3 style="${estilo}">${item.contenido}</h3>`;
                break;
                
            case 'P': // Párrafo <p>
                // Convertir saltos de línea en <br>
                const contenidoConSaltos = item.contenido.replace(/\n/g, '<br>');
                html += `<p style="${estilo}">${contenidoConSaltos}</p>`;
                break;
                
            case 'I': // Imagen <img>
                html += `
                    <div style="text-align: center; margin: 2rem 0;">
                        <img src="${item.contenido}" style="${estilo}; max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1)" 
                             alt="Imagen del blog" 
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='block'"
                             loading="lazy">
                        <div style="display: none; padding: 1rem; background: #fff3cd; color: #856404; border-radius: 4px; margin-top: 1rem;">
                            🖼️ No se pudo cargar la imagen: ${item.contenido}
                        </div>
                    </div>
                `;
                break;
                
            default:
                html += `<div style="${estilo}">${item.contenido}</div>`;
        }
    });
    
    // Agregar contador al final
    html += `
        <div style="margin-top: 3rem; padding: 1rem; background: #e8f5e8; border-radius: 8px; text-align: center;">
            <small style="color: #27ae60;">
                ✅ Contenido generado dinámicamente: ${contenido.length} elementos en ${contadorPublicaciones} publicaciones
            </small>
        </div>
    `;
    
    contenedor.innerHTML = html;
}

function mostrarErrorBlog(mensaje) {
    const contenedor = document.getElementById('contenido-blog');
    contenedor.innerHTML = `
        <div class="error-mensaje" style="text-align: center; padding: 3rem; background: #f8d7da; color: #721c24; border-radius: 8px;">
            <div style="font-size: 48px; margin-bottom: 1rem;">❌</div>
            <h3 style="color: #721c24;">Error al cargar el contenido</h3>
            <p style="margin: 1rem 0;">${mensaje}</p>
            <button onclick="cargarBlog()" class="btn-recargar">🔄 Reintentar</button>
        </div>
    `;
}

// Función para subir archivo
async function subirArchivo(event) {
    event.preventDefault();
    
    const formData = new FormData(event.target);
    const boton = event.target.querySelector('button[type="submit"]');
    const estado = document.getElementById('estado-subida');
    const archivoInput = document.getElementById('archivo');
    
    // Validar archivo
    if (!archivoInput.files || !archivoInput.files[0]) {
        mostrarMensaje('Por favor selecciona un archivo', 'error');
        return;
    }
    
    const archivo = archivoInput.files[0];
    const extension = archivo.name.split('.').pop().toLowerCase();
    
    if (!['csv', 'xlsx', 'xls'].includes(extension)) {
        mostrarMensaje('Solo se permiten archivos CSV o Excel', 'error');
        return;
    }
    
    try {
        boton.disabled = true;
        boton.innerHTML = '📤 Subiendo...';
        estado.innerHTML = `
            <div class="estado-cargando">
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <div class="spinner"></div>
                    <span>Procesando archivo: ${archivo.name}</span>
                </div>
            </div>
        `;
        
        const response = await fetch(`${API_BASE}/subir`, {
            method: 'POST',
            body: formData
        });
        
        const resultado = await response.json();
        
        if (response.ok) {
            estado.innerHTML = `
                <div class="estado-exito">
                    <div style="font-size: 24px; margin-bottom: 0.5rem;">✅</div>
                    <h4>¡Archivo procesado exitosamente!</h4>
                    <p><strong>Registro:</strong> #${resultado.registro_id}</p>
                    <p><strong>Elementos cargados:</strong> ${resultado.elementos_procesados}</p>
                    <p><strong>Tipos de contenido:</strong></p>
                    <ul>
                        ${resultado.tipos_contenido.T ? `<li>Títulos (T): ${resultado.tipos_contenido.T}</li>` : ''}
                        ${resultado.tipos_contenido.ST ? `<li>Subtítulos (ST): ${resultado.tipos_contenido.ST}</li>` : ''}
                        ${resultado.tipos_contenido.P ? `<li>Párrafos (P): ${resultado.tipos_contenido.P}</li>` : ''}
                        ${resultado.tipos_contenido.I ? `<li>Imágenes (I): ${resultado.tipos_contenido.I}</li>` : ''}
                    </ul>
                    <p style="margin-top: 1rem;"><em>Redirigiendo al blog...</em></p>
                </div>
            `;
            
            event.target.reset();
            
            // Redirigir al blog después de 3 segundos
            setTimeout(() => {
                window.location.href = 'index.html';
            }, 3000);
            
        } else {
            estado.innerHTML = `
                <div class="estado-error">
                    <div style="font-size: 24px; margin-bottom: 0.5rem;">❌</div>
                    <h4>Error al procesar el archivo</h4>
                    <p>${resultado.error}</p>
                    <p style="margin-top: 1rem; font-size: 0.9em; color: #666;">
                        Verifica que el archivo tenga el formato correcto y todas las columnas requeridas.
                    </p>
                </div>
            `;
        }
        
    } catch (error) {
        estado.innerHTML = `
            <div class="estado-error">
                <div style="font-size: 24px; margin-bottom: 0.5rem;">❌</div>
                <h4>Error de conexión</h4>
                <p>${error.message}</p>
                <p style="margin-top: 1rem; font-size: 0.9em; color: #666;">
                    Verifica tu conexión a internet e intenta nuevamente.
                </p>
            </div>
        `;
    } finally {
        boton.disabled = false;
        boton.innerHTML = 'Subir Archivo';
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
            document.getElementById('contenido-historial').innerHTML = `
                <div class="error-mensaje">
                    <p>Error al cargar el historial: ${historial.error}</p>
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('contenido-historial').innerHTML = `
            <div class="error-mensaje">
                <p>Error de conexión: ${error.message}</p>
            </div>
        `;
    }
}

// Función para mostrar historial
function mostrarHistorial(historial) {
    const contenedor = document.getElementById('contenido-historial');
    
    if (historial.length === 0) {
        contenedor.innerHTML = `
            <div class="historial-vacio" style="text-align: center; padding: 3rem;">
                <div style="font-size: 48px; margin-bottom: 1rem;">📭</div>
                <h3 style="color: #7f8c8d;">No hay registros en el historial</h3>
                <p style="color: #95a5a6; margin-bottom: 2rem;">Aún no se han subido archivos al sistema.</p>
                <a href="subir.html" class="btn-subir">📤 Subir Primer Archivo</a>
            </div>
        `;
        return;
    }
    
    let html = `
        <div class="resumen-historial" style="background: white; padding: 1.5rem; border-radius: 8px; margin-bottom: 1.5rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1)">
            <h4 style="color: #2c3e50; margin-bottom: 1rem;">📊 Resumen del Historial</h4>
            <p><strong>Total de registros:</strong> ${historial.length}</p>
            <p><strong>Última actualización:</strong> ${new Date(historial[0].fecha_actualizacion).toLocaleString('es-ES')}</p>
        </div>
        
        <table class="tabla-historial">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Archivo</th>
                    <th>Usuario</th>
                    <th>Fecha</th>
                    <th>Elementos</th>
                    <th>Publicación</th>
                    <th>Acciones</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    historial.forEach(item => {
        const fecha = new Date(item.fecha_actualizacion).toLocaleString('es-ES');
        html += `
            <tr>
                <td><strong>#${item.id}</strong></td>
                <td>${item.nombre_archivo}</td>
                <td>${item.usuario || 'Anónimo'}</td>
                <td>${fecha}</td>
                <td>${item.cantidad_registros}</td>
                <td>${item.numero_publicacion || 'N/A'}</td>
                <td>
                    <a href="detalle.html?id=${item.id}" class="btn-ver">👁️ Ver</a>
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
        document.getElementById('contenido-detalle').innerHTML = `
            <div class="error-mensaje">
                <p>No se especificó un ID de registro</p>
            </div>
        `;
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/detalle/${registroId}`);
        const data = await response.json();
        
        if (response.ok) {
            mostrarDetalle(data.registro, data.contenido);
        } else {
            document.getElementById('contenido-detalle').innerHTML = `
                <div class="error-mensaje">
                    <p>Error al cargar el detalle: ${data.error}</p>
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('contenido-detalle').innerHTML = `
            <div class="error-mensaje">
                <p>Error de conexión: ${error.message}</p>
            </div>
        `;
    }
}

// Función para mostrar detalle
function mostrarDetalle(registro, contenido) {
    const contenedor = document.getElementById('contenido-detalle');
    
    let html = `
        <div class="detalle-registro">
            <div class="header-detalle" style="background: #2c3e50; color: white; padding: 2rem; border-radius: 8px 8px 0 0;">
                <h2 style="margin: 0; color: white;">Detalle del Registro #${registro.id}</h2>
                <p style="margin: 0.5rem 0 0 0; opacity: 0.8;">Archivo: ${registro.nombre_archivo}</p>
            </div>
            
            <div class="info-registro">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem;">
                    <div>
                        <h4>📋 Información General</h4>
                        <p><strong>Usuario:</strong> ${registro.usuario || 'Anónimo'}</p>
                        <p><strong>Fecha:</strong> ${new Date(registro.fecha_actualizacion).toLocaleString('es-ES')}</p>
                        <p><strong>Estado:</strong> <span style="color: #27ae60;">${registro.estado || 'completado'}</span></p>
                    </div>
                    <div>
                        <h4>📊 Estadísticas</h4>
                        <p><strong>Elementos:</strong> ${contenido.length}</p>
                        <p><strong>Publicación:</strong> ${registro.numero_publicacion || 'N/A'}</p>
                        <p><strong>Modo:</strong> ${registro.modo_ejecucion || 'azure'}</p>
                    </div>
                </div>
                
                <h3>📝 Contenido Generado (${contenido.length} elementos):</h3>
                <div class="contenido-detalle">
    `;
    
    if (contenido.length === 0) {
        html += `
            <div style="text-align: center; padding: 2rem; background: #f8f9fa; border-radius: 8px;">
                <p style="color: #7f8c8d;">No hay contenido en este registro.</p>
            </div>
        `;
    } else {
        contenido.forEach((item, index) => {
            const estilo = item.estilo || '';
            const numeroElemento = index + 1;
            
            html += `
                <div class="elemento-contenido" style="border: 1px solid #ecf0f1; border-radius: 8px; padding: 1.5rem; margin-bottom: 1.5rem; background: white;">
                    <div style="display: flex; justify-content: between; align-items: center; margin-bottom: 1rem; padding-bottom: 0.5rem; border-bottom: 1px solid #f8f9fa;">
                        <span style="background: #3498db; color: white; padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.8em;">
                            ${numeroElemento}. ${item.tipo_contenido}
                        </span>
                        <small style="color: #7f8c8d;">
                            ${item.dia}/${item.mes}/${item.ano} - Pub. ${item.numero_publicacion}
                        </small>
                    </div>
            `;
            
            switch(item.tipo_contenido?.toUpperCase()) {
                case 'T':
                    html += `<h4 style="${estilo}">${item.contenido}</h4>`;
                    break;
                case 'ST':
                    html += `<h5 style="${estilo}">${item.contenido}</h5>`;
                    break;
                case 'P':
                    const contenidoConSaltos = item.contenido.replace(/\n/g, '<br>');
                    html += `<p style="${estilo}">${contenidoConSaltos}</p>`;
                    break;
                case 'I':
                    html += `
                        <div style="text-align: center;">
                            <img src="${item.contenido}" style="${estilo}; max-width: 300px; height: auto; border-radius: 4px;" 
                                 alt="Imagen" 
                                 onerror="this.style.display='none'">
                            <div style="margin-top: 0.5rem;">
                                <small style="color: #7f8c8d;">URL: ${item.contenido}</small>
                            </div>
                        </div>
                    `;
                    break;
                default:
                    html += `<div style="${estilo}">${item.contenido}</div>`;
            }
            
            if (estilo) {
                html += `<div style="margin-top: 0.5rem;"><small style="color: #95a5a6;"><strong>Estilo:</strong> ${estilo}</small></div>`;
            }
            
            html += `</div>`;
        });
    }
    
    html += `
                </div>
            </div>
        </div>
    `;
    
    contenedor.innerHTML = html;
}

// Función para mostrar información del sistema
async function cargarInfoSistema() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        const info = await response.json();
        
        const contenedor = document.getElementById('info-sistema');
        contenedor.innerHTML = `
            <div class="info-card">
                <h3>🖥️ Información del Sistema</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem;">
                    <div>
                        <h4>Estado del Sistema</h4>
                        <p><strong>Status:</strong> <span style="color: #27ae60;">${info.status}</span></p>
                        <p><strong>Modo:</strong> ${info.mode}</p>
                        <p><strong>Base de Datos:</strong> <span style="color: ${info.database === 'connected' ? '#27ae60' : '#e74c3c'}">${info.database}</span></p>
                        <p><strong>Versión:</strong> ${info.version}</p>
                    </div>
                    <div>
                        <h4>Estadísticas</h4>
                        <p><strong>Registros:</strong> ${info.database_info?.total_registros || 0}</p>
                        <p><strong>Elementos:</strong> ${info.database_info?.total_elementos || 0}</p>
                        <p><strong>Última verificación:</strong> ${new Date(info.timestamp).toLocaleString('es-ES')}</p>
                    </div>
                </div>
            </div>
        `;
    } catch (error) {
        document.getElementById('info-sistema').innerHTML = `
            <div class="error-mensaje">
                <p>Error al cargar información del sistema: ${error.message}</p>
            </div>
        `;
    }
}

// Utilidad para mostrar mensajes temporales
function mostrarMensaje(mensaje, tipo = 'info') {
    const mensajeDiv = document.createElement('div');
    mensajeDiv.className = `mensaje mensaje-${tipo}`;
    mensajeDiv.innerHTML = `
        <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span>${tipo === 'error' ? '❌' : tipo === 'success' ? '✅' : 'ℹ️'}</span>
            <span>${mensaje}</span>
        </div>
    `;
    
    document.body.appendChild(mensajeDiv);
    
    setTimeout(() => {
        mensajeDiv.style.opacity = '0';
        setTimeout(() => mensajeDiv.remove(), 300);
    }, 4000);
}

// Navegación activa
document.addEventListener('DOMContentLoaded', function() {
    const currentPage = window.location.pathname.split('/').pop() || 'index.html';
    const navLinks = document.querySelectorAll('nav a');
    
    navLinks.forEach(link => {
        if (link.getAttribute('href') === currentPage) {
            link.classList.add('active');
        }
    });
});

// Inicialización automática según la página
document.addEventListener('DOMContentLoaded', function() {
    const currentPage = window.location.pathname.split('/').pop();
    
    switch(currentPage) {
        case 'index.html':
        case '':
        case '/':
            cargarBlog();
            break;
        case 'historial.html':
            cargarHistorial();
            break;
        case 'detalle.html':
            cargarDetalle();
            break;
        case 'info.html':
            cargarInfoSistema();
            break;
    }
});