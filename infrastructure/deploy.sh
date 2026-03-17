#!/bin/bash
# deploy.sh — Deploy manufacturing analytics CloudFormation stack
# Usage: ./deploy.sh [dev|prod]

set -euo pipefail

ENV=${1:-dev}
STACK_NAME="manufacturing-analytics-${ENV}"
TEMPLATE="infrastructure/cloudformation.yaml"
PARAMS="infrastructure/parameters.json"
REGION=${AWS_REGION:-us-east-1}
PROFILE=${AWS_PROFILE:-default}

echo "========================================="
echo " Deploying: ${STACK_NAME}"
echo " Region:    ${REGION}"
echo " Profile:   ${PROFILE}"
echo "========================================="

# Validate template first
echo "[1/3] Validating CloudFormation template..."
aws cloudformation validate-template \
  --template-body file://${TEMPLATE} \
  --region ${REGION} \
  --profile ${PROFILE} \
  > /dev/null

echo "[2/3] Deploying stack..."
aws cloudformation deploy \
  --stack-name ${STACK_NAME} \
  --template-file ${TEMPLATE} \
  --parameter-overrides file://${PARAMS} \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION} \
  --profile ${PROFILE} \
  --tags \
      Project=manufacturing-analytics \
      Environment=${ENV}

echo "[3/3] Fetching stack outputs..."
aws cloudformation describe-stacks \
  --stack-name ${STACK_NAME} \
  --region ${REGION} \
  --profile ${PROFILE} \
  --query "Stacks[0].Outputs" \
  --output table

echo ""
echo "✅ Deployment complete: ${STACK_NAME}"

# Upload Glue script to S3
SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name ${STACK_NAME} \
  --region ${REGION} \
  --profile ${PROFILE} \
  --query "Stacks[0].Outputs[?OutputKey=='GlueScriptsBucket'].OutputValue" \
  --output text 2>/dev/null || echo "")

if [ -n "${SCRIPTS_BUCKET}" ]; then
  echo ""
  echo "Uploading Glue ETL script to s3://${SCRIPTS_BUCKET}/scripts/"
  aws s3 cp etl/glue_etl_job.py \
    s3://${SCRIPTS_BUCKET}/scripts/glue_etl_job.py \
    --region ${REGION} \
    --profile ${PROFILE}
  echo "✅ Glue script uploaded."
fi