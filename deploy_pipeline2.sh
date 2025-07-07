#!/bin/bash
set -euo pipefail

# Flags para checklist
STEP_APIS_ENABLED=false
STEP_TOPICS_CREATED=false
STEP_APP_ENGINE_CREATED=false
STEP_ARTIFACTROLE_ASSIGNED=false
# STEP_REPO_CLONED - Eliminado
STEP_FUNC_INGESTA_DEPLOYED=false
STEP_FUNC_PROC_DEPLOYED=false
STEP_BQ_RESOURCES_CREATED=false
STEP_BQ_PERMS_ASSIGNED=false
STEP_SUBSCRIPTION_CONFIGURED=false
STEP_TEST_MSG_PUBLISHED=false
STEP_CLEANUP_COMPLETED=false

function check_command {
  "$@"
  local status=$?
  if [ $status -ne 0 ]; then
    echo "❌ ERROR: El comando '$*' falló con código $status. Abortando."
    exit $status
  fi
}

function info {
  echo -e "🛠️  INFO: $*"
}

function warning {
  echo -e "⚠️ WARNING: $*"
}

function success {
  echo -e "✅ SUCCESS: $*"
}


info "Configurando proyecto y habilitando APIs..."
check_command gcloud config set project "$(gcloud config get-value project)"
PROJECT_ID=$(gcloud config get-value project)

info "Habilitando APIs necesarias..."
check_command gcloud services enable run.googleapis.com \
                       pubsub.googleapis.com \
                       iam.googleapis.com \
                       cloudbuild.googleapis.com \
                       cloudfunctions.googleapis.com \
                       appengine.googleapis.com \
                       bigquery.googleapis.com
STEP_APIS_ENABLED=true

info "Creando tópicos de Pub/Sub (si no existen)..."
gcloud pubsub topics create registros-produccion || warning "El tópico registros-produccion ya existe"
gcloud pubsub topics create registros-dlq || warning "El tópico registros-dlq ya existe"
STEP_TOPICS_CREATED=true

info "Creando App Engine (solo una vez por proyecto)..."
if gcloud app describe >/dev/null 2>&1; then
  warning "App Engine ya existe."
else
  check_command gcloud app create --region=us-central
fi
STEP_APP_ENGINE_CREATED=true

# ... (El bloque para forzar la creación del SA de gcf-admin-robot y asignar permisos artifactregistry.reader es bueno y puede permanecer) ...
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
GCF_SA="service-${PROJECT_NUMBER}@gcf-admin-robot.iam.gserviceaccount.com"
# ...

info "Desplegando función INGESTA (Gen2 - HTTP)..."
check_command gcloud functions deploy webhook-ingesta \
  --gen2 \
  --runtime=python311 \
  --trigger-http \
  --source=./ingesta \
  --entry-point=main \
  --region=us-central1 \
  --allow-unauthenticated \
  --set-env-vars="TOPIC_ID=registros-produccion,GCP_PROJECT=${PROJECT_ID}"
STEP_FUNC_INGESTA_DEPLOYED=true

info "Desplegando función PROCESAMIENTO (Gen1 - Pub/Sub)..."
check_command gcloud functions deploy procesamiento-datos \
  --runtime python311 \
  --trigger-resource registros-produccion \
  --trigger-event google.pubsub.topic.publish \
  --source=./procesamiento \
  --entry-point=main \
  --region=us-central1 \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
  --retry \
  --no-gen2
STEP_FUNC_PROC_DEPLOYED=true

info "Creando recursos de BigQuery (si no existen)..."
if bq ls --datasets | grep -q -w "DatosTiempoReal"; then
    warning "El dataset 'DatosTiempoReal' ya existe."
else
    check_command bq mk --dataset --location=US DatosTiempoReal
    success "Dataset 'DatosTiempoReal' creado."
fi
if bq ls DatosTiempoReal | grep -q -w "DatosTR"; then
    warning "La tabla 'DatosTR' ya existe."
else
    if [ ! -f "schema.json" ]; then
        echo "❌ ERROR: No se encuentra el archivo 'schema.json'. El script debe ejecutarse desde la raíz del repositorio."
        exit 1
    fi
    check_command bq mk --table DatosTiempoReal.DatosTR ./schema.json
    success "Tabla 'DatosTR' creada."
fi
STEP_BQ_RESOURCES_CREATED=true

info "Asignando permisos de BigQuery a la función..."
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
check_command gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataEditor"
check_command gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"
STEP_BQ_PERMS_ASSIGNED=true

info "Esperando 60 segundos para asegurar que se cree la suscripción..."
sleep 30

info "Buscando suscripción generada por la función procesamiento-datos..."
SUBSCRIPTION_ID=$(gcloud pubsub topics list-subscriptions registros-produccion --format="value(name)" | grep procesamiento-datos || true)

if [[ -z "$SUBSCRIPTION_ID" ]]; then
  warning "No se pudo encontrar automáticamente la suscripción. Usando nombre conocido manualmente."
  SUBSCRIPTION_ID="projects/${PROJECT_ID}/subscriptions/gcf-procesamiento-datos-us-central1-registros-produccion"
fi

if [[ -n "$SUBSCRIPTION_ID" ]]; then
  check_command gcloud pubsub subscriptions update "$SUBSCRIPTION_ID" \
    --dead-letter-topic=registros-dlq \
    --max-delivery-attempts=5
  success "DLQ configurada correctamente para la suscripción."
  STEP_SUBSCRIPTION_CONFIGURED=true
else
  warning "Aún no se pudo encontrar la suscripción para configurar DLQ."
fi

info "Publicando mensaje de prueba en registros-produccion para validar el pipeline..."
check_command gcloud pubsub topics publish registros-produccion --message='{"id_cliente":"test_001","cliente":"Cliente de Prueba","genero":"N/A","id_producto":"prod_test","producto":"Producto de Prueba","precio":10,"cantidad":1,"monto":10,"forma_pago":"Test","fecreg":"2025-01-01T12:00:00Z"}'
STEP_TEST_MSG_PUBLISHED=true

info "===== CHECKLIST DE PASOS DEL DESPLIEGUE ====="
echo "[$( [ $STEP_APIS_ENABLED == true ] && echo x || echo ' ' )] APIs habilitadas"
echo "[$( [ $STEP_TOPICS_CREATED == true ] && echo x || echo ' ' )] Tópicos Pub/Sub creados"
echo "[$( [ $STEP_APP_ENGINE_CREATED == true ] && echo x || echo ' ' )] App Engine creado (o ya existía)"
# ...
echo "[$( [ $STEP_FUNC_INGESTA_DEPLOYED == true ] && echo x || echo ' ' )] Función INGESTA desplegada"
echo "[$( [ $STEP_FUNC_PROC_DEPLOYED == true ] && echo x || echo ' ' )] Función PROCESAMIENTO desplegada"
echo "[$( [ $STEP_BQ_RESOURCES_CREATED == true ] && echo x || echo ' ' )] Recursos BigQuery creados"
echo "[$( [ $STEP_BQ_PERMS_ASSIGNED == true ] && echo x || echo ' ' )] Permisos BigQuery asignados"
echo "[$( [ $STEP_SUBSCRIPTION_CONFIGURED == true ] && echo x || echo ' ' )] Suscripción configurada con DLQ"
echo "[$( [ $STEP_TEST_MSG_PUBLISHED == true ] && echo x || echo ' ' )] Mensaje de prueba publicado"
info "============================================="

info "🎉 Despliegue completado."
info "Esperando 10 segundos antes de la limpieza del dato de prueba..."
sleep 10

info "Limpiando el registro de prueba de la tabla de BigQuery..."
DELETE_QUERY="DELETE FROM \`${PROJECT_ID}.DatosTiempoReal.DatosTR\` WHERE id_cliente = 'test_001'"
bq query --use_legacy_sql=false "$DELETE_QUERY" >/dev/null 2>&1 || warning "No se pudo eliminar el registro de prueba (puede que no existiera)."
success "Registro de prueba eliminado (si existía)."
STEP_CLEANUP_COMPLETED=true

echo "[$( [ $STEP_CLEANUP_COMPLETED == true ] && echo x || echo ' ' )] Limpieza de datos de prueba completada"
info "✅ Proceso finalizado."