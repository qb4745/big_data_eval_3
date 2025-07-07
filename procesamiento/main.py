import os
import json
import base64
from google.cloud import bigquery
from datetime import datetime, timezone

# --- Configuración ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = 'DatosTiempoReal' # Asegúrate que este sea el nombre de tu dataset
TABLE_ID = 'DatosTR'           # Asegúrate que este sea el nombre de tu tabla

client = bigquery.Client()

def main(event, context):
    """
    Función de 1ra Gen activada por Pub/Sub para procesar datos.
    """
    try:
        # 1. Decodificar el mensaje de Pub/Sub (diferente en Gen1)
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(payload_str)
        
        # --- LOG DE INICIO ---
        print(json.dumps({
            "severity": "INFO",
            "message": "Iniciando procesamiento de mensaje",
            "event_id": context.event_id,
            "transaction_id": data.get("id_transaccion", "N/A")
        }))

        # 2. Validación y Limpieza
        if "id_transaccion" not in data or "producto" not in data:
            raise ValueError("Payload del mensaje no contiene campos requeridos (id_transaccion, producto).")
        data['producto'] = data.get('producto', '').strip().upper()
        
        # 3. Enriquecimiento
        data['fecha_procesamiento_gcp'] = datetime.now(timezone.utc).isoformat()

        # 4. Construcción y ejecución de la consulta MERGE
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        merge_sql = f"""
        MERGE `{table_ref}` T
        USING (SELECT
            CAST('{data.get("id_transaccion")}' AS STRING) as id_transaccion,
            '{data.get("producto")}' as producto,
            CAST({data.get("monto", 0)}) as monto,
            '{data.get("forma_pago")}' as forma_pago,
            TIMESTAMP('{data.get("fecha_procesamiento_gcp")}') as fecha_procesamiento_gcp
        ) S
        ON T.id_transaccion = S.id_transaccion
        WHEN NOT MATCHED THEN
          INSERT (id_transaccion, producto, monto, forma_pago, fecha_procesamiento_gcp)
          VALUES(S.id_transaccion, S.producto, S.monto, S.forma_pago, S.fecha_procesamiento_gcp);
        """
        job = client.query(merge_sql)
        job.result()

        # --- LOG DE ÉXITO ---
        print(json.dumps({
            "severity": "INFO",
            "message": "Registro procesado exitosamente.",
            "event_id": context.event_id,
            "transaction_id": data.get("id_transaccion")
        }))

    except Exception as e:
        # --- LOG DE ERROR CRÍTICO ---
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Fallo crítico en el procesamiento del mensaje: {e}",
            "event_id": context.event_id,
            "original_payload": payload_str
        }))
        raise e