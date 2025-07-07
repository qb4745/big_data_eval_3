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

        print(f"INFO: Procesando registro para cliente {record_data.get('id_cliente')}")

        # 2. Limpieza y preparación de valores
        id_cliente = record_data.get("id_cliente", "")
        cliente = record_data.get('cliente', '').strip().upper().replace("'", "\\'")
        genero = record_data.get('genero', '')
        id_producto = record_data.get('id_producto', '')
        producto = record_data.get('producto', '').strip().upper().replace("'", "\\'")
        # SOLUCIÓN: Usar .get(key, 0) para valores numéricos para evitar CAST(None)
        precio = record_data.get("precio") or 0
        cantidad = record_data.get("cantidad") or 0
        monto = record_data.get("monto") or 0
        forma_pago = record_data.get('forma_pago', '').strip().upper().replace("'", "\\'")
        fecreg = record_data.get('fecreg')

        # 3. Enriquecimiento
        fecha_procesamiento_gcp = datetime.now(timezone.utc).isoformat()

        # 4. Construcción de la consulta MERGE
        table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        
        merge_sql = f"""
        MERGE {table_ref} T
        USING (SELECT
            CAST('{id_cliente}' AS STRING) as id_cliente,
            '{cliente}' as cliente,
            '{genero}' as genero,
            CAST('{id_producto}' AS STRING) as id_producto,
            '{producto}' as producto,
            CAST({precio} AS FLOAT64) as precio,
            CAST({cantidad} AS INT64) as cantidad,
            CAST({monto} AS FLOAT64) as monto,
            '{forma_pago}' as forma_pago,
            TIMESTAMP('{fecreg}') as fecreg,
            TIMESTAMP('{fecha_procesamiento_gcp}') as fecha_procesamiento_gcp
        ) S
        ON T.id_cliente = S.id_cliente AND T.fecreg = S.fecreg
        WHEN NOT MATCHED THEN
          INSERT (id_cliente, cliente, genero, id_producto, producto, precio, cantidad, monto, forma_pago, fecreg, fecha_procesamiento_gcp)
          VALUES(S.id_cliente, S.cliente, S.genero, S.id_producto, S.producto, S.precio, S.cantidad, S.monto, S.forma_pago, S.fecreg, S.fecha_procesamiento_gcp);
        """

        # 5. Ejecutar la consulta
        job = client.query(merge_sql)
        job.result()
        print(f"INFO: Registro para cliente {id_cliente} cargado exitosamente en BigQuery.")

    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Fallo al procesar registro individual: {e}",
            "event_id": context.event_id,
            "failed_record": str(record_data)
        }))

def main(event, context):
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