import os
import json
import base64
from google.cloud import bigquery
from datetime import datetime, timezone

# --- Configuración ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = 'DatosTiempoReal'
TABLE_ID = 'DatosTR'

client = bigquery.Client()

def process_single_record(record_data, context):
    """
    Función auxiliar que procesa un único registro (diccionario).
    """
    try:
        # --- LOG DE INICIO DE REGISTRO ---
        print(json.dumps({
            "severity": "INFO",
            "message": "Procesando registro individual",
            "event_id": context.event_id,
            "transaction_id": record_data.get("id_transaccion", "N/A")
        }))

        # 2. Validación y Limpieza
        if "id_transaccion" not in record_data:
            raise ValueError("Registro no contiene 'id_transaccion'.")
        record_data['producto'] = record_data.get('producto', '').strip().upper()
        
        # 3. Enriquecimiento
        record_data['fecha_procesamiento_gcp'] = datetime.now(timezone.utc).isoformat()

        # 4. Construcción y ejecución de la consulta MERGE
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        # NOTA: Ajusta las columnas según tu esquema de BigQuery
        merge_sql = f"""
        MERGE `{table_ref}` T
        USING (SELECT
            CAST('{record_data.get("id_transaccion")}' AS STRING) as id_transaccion,
            '{record_data.get("producto")}' as producto,
            CAST({record_data.get("monto", 0)}) as monto,
            '{record_data.get("forma_pago")}' as forma_pago,
            TIMESTAMP('{record_data.get("fecha_procesamiento_gcp")}') as fecha_procesamiento_gcp
        ) S
        ON T.id_transaccion = S.id_transaccion
        WHEN NOT MATCHED THEN
            INSERT (id_transaccion, producto, monto, forma_pago, fecha_procesamiento_gcp)
            VALUES(S.id_transaccion, S.producto, S.monto, S.forma_pago, S.fecha_procesamiento_gcp);
        """
        job = client.query(merge_sql)
        job.result()

    except Exception as e:
        # Log del error específico del registro, pero no detenemos el lote entero
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Fallo al procesar registro individual: {e}",
            "event_id": context.event_id,
            "failed_record": str(record_data) # Convertimos a string por si no es serializable
        }))
        # Opcional: Podríamos querer seguir procesando otros registros del lote
        # En este caso, no relanzamos la excepción para no matar el procesamiento del lote.
        # raise e 

def main(event, context):
    """
    Función de 1ra Gen que puede manejar un único evento o un lote de eventos.
    """
    try:
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(payload_str)
        
        if isinstance(data, list):
            # El payload es una lista (lote), iteramos sobre cada registro
            print(f"INFO: Lote de {len(data)} registros recibido. Procesando uno por uno.")
            for record in data:
                process_single_record(record, context)
        elif isinstance(data, dict):
            # El payload es un único objeto (diccionario)
            process_single_record(data, context)
        else:
            raise TypeError(f"Tipo de payload no soportado: {type(data)}")

    except Exception as e:
        # Este error es para fallos generales (ej. JSON malformado)
        print(json.dumps({
            "severity": "CRITICAL",
            "message": f"Fallo irrecuperable en la función principal: {e}",
            "event_id": context.event_id,
            "original_payload": payload_str
        }))
        raise e