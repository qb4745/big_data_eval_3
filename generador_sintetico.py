import json
import os
import random
import time
import uuid
from google.cloud import pubsub_v1

# --- CONFIGURACIÓN ---
# Leemos la configuración desde variables de entorno para mantener las buenas prácticas.
try:
    project_id = os.environ['PROJECT_ID']
    topic_id = os.environ['TOPIC_ID']
except KeyError as e:
    raise RuntimeError(f"Variable de entorno requerida no encontrada: {e}. Por favor, ejecute con 'export ...'")

# --- CLIENTE DE PUBSUB ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# --- DATOS DE MUESTRA AMPLIADOS Y REALISTAS ---
# Hemos actualizado estas listas con la data de tu tabla de BigQuery para una simulación de alta fidelidad.
CLIENTES = [
    (1, "RAUL OPAZO", "H"),
    (2, "ALEJANDRO PÉREZ", "H"),
    (3, "FELIPE MUÑOZ", "H"),
    (5, "MAURICIO CORREA", "H"),
    (6, "CAROLINA LÓPEZ", "M"),
    (8, "PAOLA ROJAS", "M"),
    (9, "MARLÉN SOTO", "M"),
    (10, "LUISA TORRES", "M"),
    # Añadimos algunos clientes ficticios más para mayor variedad
    (11, "SOFIA VERGARA", "M"),
    (12, "JAVIER BARDEM", "H"),
    (13, "ISABEL ALLENDE", "M"),
    (14, "ANTONIO BANDERAS", "H")
]

PRODUCTOS = [
    (1, "AMAZON", 151.48),
    (2, "NVIDIA", 537.94),
    (3, "META", 359.09),
    (4, "ALPHABET", 141.25),
    (5, "MICROSOFT", 375.60),
    (6, "DELTA", 42.22),
    (7, "APPLE", 184.85),
    (8, "SALESFORCE", 263.09),
    (9, "DISNEY", 90.24),
    (10, "CISCO", 49.74)
]

FORMAS_PAGO = ["CRÉDITO", "DÉBITO", "EFECTIVO"]

def crear_registro_sintetico():
    """Crea un único registro de venta de subasta con datos aleatorios pero realistas."""
    
    id_cliente, cliente, genero = random.choice(CLIENTES)
    id_producto, producto, precio_base = random.choice(PRODUCTOS)
    
    cantidad = random.randint(1, 100)
    precio_final = round(precio_base * random.uniform(0.95, 1.05), 2)
    monto_total = round(precio_final * cantidad, 2)
    
    # El payload debe coincidir exactamente con el esquema de la tabla de destino.
    registro = {
        "event_id": str(uuid.uuid4()),  # ID único para la idempotencia y deduplicación
        "id_cliente": id_cliente,
        "cliente": cliente,
        "genero": genero,
        "id_producto": id_producto,
        "producto": producto,
        "precio": precio_final,
        "cantidad": cantidad,
        "monto": monto_total, # Aseguramos que el campo se llame 'monto'
        "forma_pago": random.choice(FORMAS_PAGO),
        "fecreg": time.strftime('%Y-%m-%d %H:%M:%S') # Fecha y hora del evento
    }
    
    return registro

# --- BUCLE PRINCIPAL ---
if __name__ == "__main__":
    print(f"🚀 Iniciando generador de datos sintéticos (v2.0 - Datos ampliados).")
    print(f"Publicando en el tópico: {topic_path}")
    print("Presiona Ctrl+C para detener.")

    try:
        while True:
            nuevo_registro = crear_registro_sintetico()
            message_bytes = json.dumps(nuevo_registro).encode("utf-8")
            
            future = publisher.publish(topic_path, message_bytes)
            future.result()
            
            print(f"✅ Publicado: '{nuevo_registro['cliente']}' compró {nuevo_registro['cantidad']} de '{nuevo_registro['producto']}'.")
            
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el generador de datos.")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")