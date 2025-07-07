import os
import json
import base64
from datetime import datetime
import functions_framework
from google.cloud import bigquery

PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = "DatosTiempoReal"
TABLE_ID = "DatosTR"

client = bigquery.Client()
table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

@functions_framework.cloud_event
def main(cloud_event):
    try:
        payload_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        data = json.loads(payload_str)
        
        print(f"Procesando mensaje con ID de evento: {cloud_event['id']}")

        required_fields = ["id_cliente", "id_producto", "fecreg"]
        if not all(field in data for field in required_fields):
            raise ValueError("Faltan campos requeridos en el mensaje.")

        precio_float = float(data.get("precio", "0").strip())
        cantidad_int = int(data.get("cantidad", 0))
        monto_float = float(data.get("monto", "0").strip())
        
        cliente_clean = data.get('cliente', '').strip()
        producto_clean = data.get('producto', '').strip()
        forma_pago_clean = data.get('forma_pago', '').upper().strip()
        fecha_procesamiento_gcp_iso = datetime.utcnow().isoformat()
        
        merge_sql = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
        USING (SELECT CAST("{data.get('id_cliente')}" AS STRING) as id_cliente, CAST("{data.get('id_producto')}" AS STRING) as id_producto, CAST("{data.get('fecreg')}" AS TIMESTAMP) as fecreg) S
        ON T.id_cliente = S.id_cliente AND T.id_producto = S.id_producto AND T.fecreg = S.fecreg
        WHEN NOT MATCHED THEN
          INSERT (id_cliente, cliente, genero, id_producto, producto, precio, cantidad, monto, forma_pago, fecreg, fecha_procesamiento_gcp)
          VALUES(
            "{data.get('id_cliente')}", "{cliente_clean}", "{data.get('genero', '')}", "{data.get('id_producto')}",
            "{producto_clean}", {precio_float}, {cantidad_int}, {monto_float},
            "{forma_pago_clean}", CAST("{data.get('fecreg')}" AS TIMESTAMP), CAST("{fecha_procesamiento_gcp_iso}" AS TIMESTAMP)
          )
        """
        job = client.query(merge_sql)
        job.result()

        print(f"Éxito: El mensaje fue procesado y cargado/fusionado en BigQuery. Job ID: {job.job_id}")

    except Exception as e:
        print(f"ERROR al procesar el mensaje: {e}")
        raise e