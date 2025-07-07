from google.cloud import pubsub_v1
import json
import os

# --- INICIALIZACIÓN ROBUSTA ---
# En lugar de depender de una variable mágica de GCP, requerimos explícitamente
# que el PROJECT_ID y TOPIC_ID sean proporcionados como variables de entorno.
# Usamos os.environ['KEY'] en lugar de .get(). Esto es mejor porque si la variable
# no está definida durante el despliegue, el servicio fallará al iniciar,
# alertándonos inmediatamente del problema de configuración.
try:
    project_id = os.environ['PROJECT_ID']
    
    topic_id = os.environ['TOPIC_ID']
except KeyError as e:
    # Este error solo ocurrirá si despliegas sin las variables de entorno correctas.
    raise RuntimeError(f"Variable de entorno requerida no encontrada: {e}. Por favor, despliegue con --set-env-vars.")

# Crear el cliente y la ruta del tópico una sola vez (como ya lo tenías, ¡bien hecho!)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def main(request):
    """
    Función que recibe un lote de registros y los publica individualmente.
    Incluye manejo de errores mejorado para depuración.
    """
    if not request.content_length:
        print("Petición de verificación (handshake) recibida. Respondiendo OK.")
        return "Endpoint verificado", 200

    try:
        data = request.get_json(silent=True)
        if data is None:
            # Captura de JSON malformado
            error_payload = request.get_data(as_text=True)
            print(f"Error: Payload no es un JSON válido. Payload recibido: {error_payload}")
            return "Error: Payload no es un JSON válido.", 400

        # Procesar lote o registro único
        records = data if isinstance(data, list) else [data]
        print(f"Recibidos {len(records)} registros. Publicando en el tópico: {topic_path}")

        for i, item in enumerate(records):
            try:
                message_bytes = json.dumps(item).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                future.result()  # Espera síncrona, como en el ejemplo original.
                # print(f"Publicado registro #{i+1}") # Descomentar para depuración muy detallada
            except Exception as e:
                # ¡NUEVO! Captura de error por CADA mensaje.
                # Esto nos dirá si un solo mensaje del lote está corrupto.
                print(f"Error al publicar el registro #{i+1} del lote: {item}. Error: {e}")
                # En un escenario real, podríamos enviar este mensaje fallido a una DLQ.
                # Por ahora, continuamos con el siguiente.
                continue

        print("Proceso completado exitosamente.")
        return "Completado", 200

    except Exception as e:
        # Captura de errores generales.
        print(f"Error inesperado al procesar la solicitud. Tópico usado: {topic_path}. Error: {e}")
        return f"Error al procesar la solicitud: {e}", 500