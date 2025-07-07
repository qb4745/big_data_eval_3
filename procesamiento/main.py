import os
import json
import functions_framework
from google.cloud import bigquery
from datetime import datetime, timezone

# --- Configuración ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = 'DatosTiempoReal' # Asegúrate que este sea el nombre de tu dataset
TABLE_ID = 'DatosTR'           # Asegúrate que este sea el nombre de tu tabla

# Inicializamos el cliente de BigQuery
client = bigquery.Client()

@functions_framework.cloud_event
def main(cloud_event):
    """
    Función activada por Pub/Sub para procesar, limpiar y cargar datos en BigQuery.
    """
    try:
        # 1. Decodificar el mensaje de Pub/Sub
        payload_str = cloud_event.data["message"]["data"].decode("utf-8")
        data = json.loads(payload_str)
        
        # --- LOG DE INICIO ---
        print(json.dumps({
            "severity": "INFO",
            "message": "Iniciando procesamiento de mensaje",
            "transaction_id": data.get("id_transaccion", "N/A")
        }))

        # 2. Validación y Limpieza (Ejemplo)
        if "id_transaccion" not in data or "producto" not in data:
            raise ValueError("Payload del mensaje no contiene campos requeridos (id_transaccion, producto).")

        # Limpiamos el nombre del producto
        data['producto'] = data.get('producto', '').strip().upper()
        
        # 3. Enriquecimiento
        data['fecha_procesamiento_gcp'] = datetime.now(timezone.utc).isoformat()

        # 4. Construcción de la consulta MERGE para deduplicación
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Asumimos que la tabla ya existe con las columnas correctas
        # NOTA: Asegúrate que las claves del dict 'data' coincidan con los nombres de las columnas en BigQuery
        # Si tu tabla tiene columnas diferentes, ajusta los nombres aquí
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

        # 5. Ejecutar la consulta
        job = client.query(merge_sql)
        job.result()  # Espera a que el job termine

        # --- LOG DE ÉXITO ---
        print(json.dumps({
            "severity": "INFO",
            "message": "Registro procesado y cargado/actualizado en BigQuery exitosamente.",
            "transaction_id": data.get("id_transaccion"),
            "bigquery_job_id": job.job_id
        }))

    except Exception as e:
        # --- LOG DE ERROR CRÍTICO ---
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Fallo crítico en el procesamiento del mensaje: {e}",
            "original_payload": payload_str
        }))
        # Re-lanzar la excepción es CRUCIAL para que Pub/Sub sepa que el procesamiento falló
        # y active la política de reintentos y la DLQ.
        raise e