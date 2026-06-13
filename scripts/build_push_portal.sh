#!/usr/bin/env bash
# Build the portal image (multi-stage: React build -> FastAPI runtime) and push
# it to the ECR repo Terraform created in the logging account. Builds for
# linux/amd64 so it runs on the x86_64 EC2 host regardless of the build machine.
set -euo pipefail
cd "$(dirname "$0")/.."

REGION="${AWS_REGION:-us-east-1}"
# Prefer the Terraform output; fall back to querying ECR directly so the push
# still works from a checkout without initialized Terraform state.
REPO="$(cd infra && terraform output -raw ecr_repo_url 2>/dev/null || true)"
if [ -z "$REPO" ]; then
  REPO="$(aws ecr describe-repositories --repository-names logstream-portal \
    --query 'repositories[0].repositoryUri' --output text)"
fi
REGISTRY="${REPO%%/*}"                                      # <acct>.dkr.ecr.<region>.amazonaws.com

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

docker build --platform linux/amd64 -t "$REPO:latest" .
docker push "$REPO:latest"
echo "pushed $REPO:latest"
