import os
import json
import base64
from google.cloud import bigquery
from datetime import datetime, timezone

# --- CONFIGURACIÓN ROBUSTA (Mejora #3) ---
# Requerimos explícitamente las variables de entorno para que la función falle al iniciar
# si no están configuradas, lo cual es una buena práctica (fail-fast).
try:
    PROJECT_ID = os.environ['PROJECT_ID']
    DATASET_ID = os.environ['DATASET_ID']
    TABLE_ID = os.environ['TABLE_ID']
except KeyError as e:
    raise RuntimeError(f"Variable de entorno requerida no encontrada: {e}. Por favor, despliegue con --set-env-vars.")

client = bigquery.Client()
table_ref_str = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"

# Añadimos una tabla para los registros fallidos (Dead-Letter Table)
# Esto es opcional pero muy recomendado para la robustez.
DEAD_LETTER_TABLE_ID = f"{TABLE_ID}_fallidos"
dead_letter_table_ref_str = f"`{PROJECT_ID}.{DATASET_ID}.{DEAD_LETTER_TABLE_ID}`"

def main(event, context):
    """
    Función principal que procesa un mensaje de Pub/Sub.
    Decodifica el payload y lo envía a la función de procesamiento.
    """
    try:
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        record_data = json.loads(payload_str)

        # 1. Validación de la clave de deduplicación (Mejora #1)
        if "event_id" not in record_data:
            raise ValueError("Registro no contiene 'event_id', campo clave para la deduplicación.")

        print(f"INFO: Procesando evento {record_data['event_id']}")
        
        # 2. Limpieza y preparación de datos
        # El generador siempre envía un 'event_id', así que no necesitamos .get()
        row_to_insert = {
            "event_id": record_data["event_id"],
            "id_cliente": str(record_data.get("id_cliente")), # Asegurar como string
            "cliente": str(record_data.get('cliente', '')).strip().upper(),
            "genero": str(record_data.get('genero', '')),
            "id_producto": str(record_data.get('id_producto')), # Asegurar como string
            "producto": str(record_data.get('producto', '')).strip().upper(),
            "precio": float(record_data.get("precio", 0)),
            "cantidad": int(record_data.get("cantidad", 0)),
            "monto": float(record_data.get("monto", 0)),
            "forma_pago": str(record_data.get('forma_pago', '')).strip().upper(),
            "fecreg": datetime.strptime(record_data.get('fecreg'), '%Y-%m-%d %H:%M:%S'),
            "fecha_procesamiento_gcp": datetime.now(timezone.utc)
        }

        # 3. Construcción de la consulta MERGE parametrizada (Mejora #2)
        merge_sql = f"""
        MERGE {table_ref_str} T
        USING (SELECT @event_id as event_id) S ON T.event_id = S.event_id
        WHEN NOT MATCHED THEN
          INSERT (event_id, id_cliente, cliente, genero, id_producto, producto, precio, cantidad, monto, forma_pago, fecreg, fecha_procesamiento_gcp)
          VALUES(
              @event_id, @id_cliente, @cliente, @genero, @id_producto, @producto, 
              @precio, @cantidad, @monto, @forma_pago, @fecreg, @fecha_procesamiento_gcp
          );
        """

        # 4. Definir los parámetros de la consulta
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(name, "STRING", value) for name, value in row_to_insert.items() if isinstance(value, str)
            ] + [
                bigquery.ScalarQueryParameter(name, "FLOAT64", value) for name, value in row_to_insert.items() if isinstance(value, float)
            ] + [
                bigquery.ScalarQueryParameter(name, "INT64", value) for name, value in row_to_insert.items() if isinstance(value, int)
            ] + [
                bigquery.ScalarQueryParameter(name, "TIMESTAMP", value) for name, value in row_to_insert.items() if isinstance(value, datetime)
            ]
        )

        # 5. Ejecutar la consulta
        query_job = client.query(merge_sql, job_config=job_config)
        query_job.result()  # Esperar a que el job termine

        print(f"INFO: Evento {row_to_insert['event_id']} cargado/ignorado exitosamente.")

    except Exception as e:
        # Si algo falla, registramos el error de forma estructurada.
        # En un escenario real, podríamos insertar el payload fallido en una tabla DLQ.
        error_payload = {
            "severity": "CRITICAL",
            "message": f"Fallo irrecuperable en la función: {e}",
            "event_id": context.event_id,
            "original_payload": payload_str,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        print(json.dumps(error_payload))
        # No relanzamos el error (no hacemos `raise e`) para que Pub/Sub no intente
        # re-enviar un mensaje que ya sabemos que es problemático.