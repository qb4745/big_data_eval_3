import os
import json
import base64
from google.cloud import bigquery
from datetime import datetime, timezone

# --- CONFIGURACIÓN ROBUSTA ---
try:
    PROJECT_ID = os.environ['PROJECT_ID']
    DATASET_ID = os.environ['DATASET_ID']
    TABLE_ID = os.environ['TABLE_ID']
except KeyError as e:
    raise RuntimeError(f"Variable de entorno requerida no encontrada: {e}. Por favor, despliegue con --set-env-vars.")

client = bigquery.Client()
table_id_full = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def main(event, context):
    """
    Función principal que procesa un mensaje de Pub/Sub usando el método de streaming de BigQuery.
    Este enfoque es mucho más eficiente que ejecutar un MERGE por cada fila.
    """
    try:
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        record_data = json.loads(payload_str)

        # 1. Validación de la clave de deduplicación
        if "event_id" not in record_data:
            raise ValueError("Registro no contiene 'event_id', campo clave.")

        print(f"INFO: Procesando evento {record_data['event_id']}")
        
        # 2. Limpieza y preparación de la fila para la inserción
        # El método insert_rows_json es más flexible con los tipos de datos.
        row_to_insert = {
            "event_id": record_data["event_id"],
            "id_cliente": str(record_data.get("id_cliente")),
            "cliente": str(record_data.get('cliente', '')).strip().upper(),
            "genero": str(record_data.get('genero', '')),
            "id_producto": str(record_data.get('id_producto')),
            "producto": str(record_data.get('producto', '')).strip().upper(),
            "precio": float(record_data.get("precio", 0)),
            "cantidad": int(record_data.get("cantidad", 0)),
            "monto": float(record_data.get("monto", 0)),
            "forma_pago": str(record_data.get('forma_pago', '')).strip().upper(),
            "fecreg": record_data.get('fecreg'), # Se inserta como STRING, BigQuery lo convierte
            "fecha_procesamiento_gcp": datetime.now(timezone.utc).isoformat()
        }

        # 3. Insertar la fila usando el método de streaming optimizado
        # Pasamos la fila dentro de una lista, ya que el método puede aceptar múltiples filas.
        errors = client.insert_rows_json(table_id_full, [row_to_insert])

        # 4. Manejo de errores de la inserción
        if not errors:
            print(f"INFO: Evento {row_to_insert['event_id']} transmitido exitosamente a BigQuery.")
        else:
            print(f"ERROR: No se pudo insertar la fila en BigQuery. Errores: {errors}")

    except Exception as e:
        error_payload = {
            "severity": "CRITICAL",
            "message": f"Fallo irrecuperable en la función: {e}",
            "event_id": context.event_id,
            "original_payload": payload_str
        }
        print(json.dumps(error_payload))