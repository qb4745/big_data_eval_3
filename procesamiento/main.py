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
        # 1. Validación de campos clave
        if "id_cliente" not in record_data or "fecreg" not in record_data:
            raise ValueError("Registro no contiene 'id_cliente' o 'fecreg', campos necesarios para la deduplicación.")

        # Log de inicio
        print(json.dumps({
            "severity": "INFO",
            "message": "Procesando registro individual",
            "event_id": context.event_id,
            "id_cliente": record_data.get("id_cliente")
        }))

        # 2. Limpieza de Datos
        # Estandarizamos el nombre del cliente a mayúsculas
        cliente_limpio = record_data.get('cliente', '').strip().upper()
        # Escapamos las comillas simples para evitar errores de SQL
        cliente_limpio = cliente_limpio.replace("'", "\\'")

        # 3. Enriquecimiento
        # Añadimos un timestamp de cuando fue procesado en GCP
        fecha_procesamiento_gcp = datetime.now(timezone.utc).isoformat()

        # 4. Construcción de la consulta MERGE para deduplicación
        table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        
        # Usamos una clave compuesta (id_cliente + fecreg) para la unicidad
        # Esto previene que se inserte el mismo evento para el mismo cliente en el mismo segundo.
        merge_sql = f"""
        MERGE {table_ref} T
        USING (SELECT
            CAST('{record_data.get("id_cliente")}' AS STRING) as id_cliente,
            '{cliente_limpio}' as cliente,
            '{record_data.get("genero")}' as genero,
            CAST('{record_data.get("id_producto")}' AS STRING) as id_producto,
            '{record_data.get("producto")}' as producto,
            CAST({record_data.get("precio", 0)}) as FLOAT64,
            CAST({record_data.get("cantidad", 0)}) as INT64,
            CAST({record_data.get("monto", 0)}) as FLOAT64,
            '{record_data.get("forma_pago")}' as forma_pago,
            TIMESTAMP('{record_data.get("fecreg")}') as fecreg,
            TIMESTAMP('{fecha_procesamiento_gcp}') as fecha_procesamiento_gcp
        ) S
        ON T.id_cliente = S.id_cliente AND T.fecreg = S.fecreg
        WHEN NOT MATCHED THEN
          INSERT (id_cliente, cliente, genero, id_producto, producto, precio, cantidad, monto, forma_pago, fecreg, fecha_procesamiento_gcp)
          VALUES(S.id_cliente, S.cliente, S.genero, S.id_producto, S.producto, S.precio, S.cantidad, S.monto, S.fecreg, S.fecha_procesamiento_gcp);
        """

        # 5. Ejecutar la consulta
        job = client.query(merge_sql)
        job.result()  # Espera a que el job termine

    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Fallo al procesar registro individual: {e}",
            "event_id": context.event_id,
            "failed_record": str(record_data)
        }))
        # No relanzamos la excepción para permitir que otros registros del lote se procesen

def main(event, context):
    """
    Función de 1ra Gen que puede manejar un único evento o un lote de eventos.
    """
    try:
        payload_str = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(payload_str)
        
        if isinstance(data, list):
            print(f"INFO: Lote de {len(data)} registros recibido. Procesando uno por uno.")
            for record in data:
                process_single_record(record, context)
        elif isinstance(data, dict):
            process_single_record(data, context)
        else:
            raise TypeError(f"Tipo de payload no soportado: {type(data)}")

    except Exception as e:
        print(json.dumps({
            "severity": "CRITICAL",
            "message": f"Fallo irrecuperable en la función principal: {e}",
            "event_id": context.event_id,
            "original_payload": payload_str
        }))
        raise e