#!/bin/bash
set -euo pipefail

PROJECT="certifyos-development"
SERVICE="certifyos-demo"
REGION="us-central1"

echo "Building and deploying $SERVICE..."

# Build
gcloud builds submit --tag gcr.io/$PROJECT/$SERVICE:latest --project=$PROJECT .

# Deploy
gcloud run deploy $SERVICE \
  --image gcr.io/$PROJECT/$SERVICE:latest \
  --region $REGION \
  --project $PROJECT \
  --platform managed \
  --memory 2Gi --cpu 2 \
  --timeout 300 \
  --concurrency 10 \
  --min-instances 1 --max-instances 3 \
  --set-secrets="/secrets/bqsaprd=bqsaprd:latest" \
  --set-env-vars="\
UR2_ENABLE_GEMINI=true,\
UR2_DEMO_MODE=true,\
UR2_GEMINI_FLASH_MODEL=gemini-2.5-flash,\
UR2_GEMINI_PRO_MODEL=gemini-2.5-pro,\
UR2_GEMINI_LOCATION=us-central1,\
UR2_LLM_ANALYSIS_PROVIDER_ORDER=gemini_vertex,\
UR2_LLM_VERIFIER_PROVIDER_ORDER=gemini_vertex,\
UR2_LLM_GENERATION_PROVIDER_ORDER=gemini_vertex,\
UR2_ENABLE_CLAUDE_VERIFIER=false,\
UR2_QUALITY_AUDIT_BQ_ENABLED=true,\
UR2_QUALITY_AUDIT_BQ_PROJECT_ID=certifyos-production-platform,\
UR2_QUALITY_AUDIT_BQ_DATASET=nppes_data,\
UR2_KNOWLEDGE_BASE_DIR=/app/knowledge_base,\
UR2_WORKSPACE_DIR=/tmp/workspace,\
UR2_DEFAULT_OUTPUT_DIR=/tmp/generated,\
PSV_SERVICE_ACCOUNT_KEY_PATH=/secrets/bqsaprd" \
  --allow-unauthenticated

URL=$(gcloud run services describe $SERVICE --region $REGION --project $PROJECT --format='value(status.url)')
echo ""
echo "Deployed! URL: $URL"
