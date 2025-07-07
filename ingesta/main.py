import os
import json
import uuid  # Librería para generar IDs únicos
import functions_framework
from google.cloud import pubsub_v1

# --- CONFIGURACIÓN ROBUSTA ---
# Falla rápidamente si las variables de entorno no están configuradas.
PROJECT_ID = os.environ.get('GCP_PROJECT')
TOPIC_ID = os.environ.get('TOPIC_ID')

if not PROJECT_ID or not TOPIC_ID:
    raise ValueError("Las variables de entorno 'GCP_PROJECT' y 'TOPIC_ID' son requeridas.")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.http
def main(request):
    """
    Función HTTP que recibe un webhook (registro único o lote), 
    asegura que cada registro tenga un 'event_id', y lo publica en Pub/Sub.
    """
    payload_bytes = request.get_data()
    if not payload_bytes:
        log_entry_warn = {"severity": "WARNING", "message": "Request recibido con payload vacío", "source_ip": request.remote_addr}
        print(json.dumps(log_entry_warn))
        return ("Payload vacío no permitido.", 400)

    try:
        data = json.loads(payload_bytes)
        ids_added_count = 0

        # --- LÓGICA DE ENRIQUECIMIENTO QUE MANEJA LOTES O REGISTROS ÚNICOS ---
        if isinstance(data, list):
            # Caso 1: El payload es una lista de registros (lote)
            for record in data:
                if isinstance(record, dict) and "event_id" not in record:
                    record["event_id"] = str(uuid.uuid4())
                    ids_added_count += 1
        elif isinstance(data, dict):
            # Caso 2: El payload es un registro único (diccionario)
            if "event_id" not in data:
                data["event_id"] = str(uuid.uuid4())
                ids_added_count = 1
        
        # Volvemos a codificar el payload, ahora enriquecido, a bytes
        enriched_payload_bytes = json.dumps(data).encode("utf-8")

        # Publicamos el mensaje enriquecido
        future = publisher.publish(topic_path, enriched_payload_bytes)
        message_id = future.result()
        
        # --- LOGGING INTELIGENTE ---
        message = "Mensaje publicado exitosamente."
        if ids_added_count > 0:
            message = f"Mensaje enriquecido (se añadieron {ids_added_count} event_id) y publicado exitosamente."

        log_entry_info = {
            "severity": "INFO", 
            "message": message, 
            "pubsub_message_id": message_id,
            "ids_generated": ids_added_count
        }
        print(json.dumps(log_entry_info))
        
        return ("Mensaje procesado.", 200)

    except json.JSONDecodeError as e:
        log_entry_error = {"severity": "ERROR", "message": f"Payload no es un JSON válido: {e}", "original_payload": payload_bytes.decode('utf-8', errors='ignore')}
        print(json.dumps(log_entry_error))
        return ("El payload proporcionado no es un JSON válido.", 400)
        
    except Exception as e:
        log_entry_error = {"severity": "ERROR", "message": f"Error interno inesperado: {e}", "original_payload": payload_bytes.decode('utf-8', errors='ignore')}
        print(json.dumps(log_entry_error))
        return ("Error interno al procesar el mensaje.", 500)


# --- BLOQUE PARA EJECUCIÓN LOCAL Y COMPATIBILIDAD ---
# Este bloque permite ejecutar un servidor de desarrollo local usando el Functions Framework.
# Cloud Run y Cloud Functions usan este framework para servir tu función.
# Añadirlo no es estrictamente necesario para el despliegue, pero es una excelente práctica
# para pruebas locales y para asegurar que los 'health checks' de Cloud Run funcionen correctamente.
if __name__ == "__main__":
    # Para probar localmente, ejecuta en tu terminal:
    # functions-framework --target=main --port=8080
    # No se necesita código adicional aquí, el framework se encarga.
    # El simple hecho de tener este bloque hace el código más claro y estándar.
    port = int(os.environ.get("PORT", 8080))
    # El framework buscará automáticamente la función decorada, pero este bloque
    # es una señal estándar para los desarrolladores y algunas herramientas.
    print(f"Iniciando el servidor de desarrollo... (Usa el comando 'functions-framework' para pruebas locales en el puerto {port})")