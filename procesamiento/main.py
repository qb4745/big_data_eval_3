import os
import json
import base64
from google.cloud import bigquery
from datetime import datetime, timezone

# --- CONFIGURACIÓN (sin cambios) ---
try:
    PROJECT_ID = os.environ['PROJECT_ID']
    DATASET_ID = os.environ['DATASET_ID']
    TABLE_ID = os.environ['TABLE_ID']
except KeyError as e:
    raise RuntimeError(f"Variable de entorno requerida no encontrada: {e}.")

client = bigquery.Client()
table_id_full = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def process_record(record_data):
    """
    Función auxiliar que procesa y prepara UN SOLO registro.
    """
    # 1. Validación de la clave de deduplicación
    if "event_id" not in record_data:
        # Ya no lanzamos un error crítico, solo lo registramos y lo saltamos.
        print(f"WARNING: Registro omitido por no contener 'event_id'. Contenido: {record_data}")
        return None

    # 2. Limpieza y preparación de la fila
    try:
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
            "fecreg": record_data.get('fecreg'),
            "fecha_procesamiento_gcp": datetime.now(timezone.utc).isoformat()
        }
        return row_to_insert
    except (ValueError, TypeError) as e:
        print(f"ERROR: No se pudo procesar el registro {record_data.get('event_id', 'SIN_ID')} debido a un problema de tipo de dato: {e}")
        return None


def main(event, context):
    """
    Función principal que procesa un mensaje de Pub/Sub, AHORA CAPAZ DE MANEJAR LOTES.
    """
    rows_to_insert = []
    try:
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(payload_str)

        # --- LÓGICA CLAVE: DETECTAR SI ES UN LOTE O UN SOLO REGISTRO ---
        if isinstance(data, list):
            # Es un lote, iteramos sobre cada registro
            print(f"INFO: Procesando un lote de {len(data)} registros.")
            for record in data:
                processed_row = process_record(record)
                if processed_row:
                    rows_to_insert.append(processed_row)
        elif isinstance(data, dict):
            # Es un solo registro
            print(f"INFO: Procesando un único registro.")
            processed_row = process_record(data)
            if processed_row:
                rows_to_insert.append(processed_row)
        else:
            raise TypeError(f"El formato del payload no es ni una lista ni un diccionario. Tipo: {type(data)}")

        # 3. Insertar todas las filas válidas en BigQuery de una sola vez
        if not rows_to_insert:
            print("INFO: No hay filas válidas para insertar después del procesamiento.")
            return

        errors = client.insert_rows_json(table_id_full, rows_to_insert)

        # 4. Manejo de errores de la inserción
        if not errors:
            print(f"INFO: {len(rows_to_insert)} registros transmitidos exitosamente a BigQuery.")
        else:
            # Este es un error a nivel de API de BigQuery, es serio.
            print(f"ERROR CRÍTICO: No se pudieron insertar las filas en BigQuery. Errores: {errors}")

    except Exception as e:
        # Este error captura fallos en la decodificación, JSON, etc.
        error_payload = {
            "severity": "CRITICAL",
            "message": f"Fallo irrecuperable en la función: {e}",
            "event_id_pubsub": context.event_id, # Usamos el ID del mensaje de Pub/Sub
            "original_payload": payload_str if 'payload_str' in locals() else "No se pudo decodificar."
        }
        print(json.dumps(error_payload))
        # Volvemos a lanzar el error para que Pub/Sub reintente o lo envíe a la DLQ.
        raise e