.PHONY: test infra-up infra-down infra-down-all build-push seed-uc seed-cribl

# ── local checks (no AWS) ─────────────────────────────────────────────
test:
	cd backend && python3 -m pytest -q
	cd frontend && npm test

# ── AWS lifecycle (3-account Cribl deployment) ───────────────────────
# Spend starts at infra-up. Two teardown paths (the portal data EBS volume has
# prevent_destroy, so a plain `terraform destroy` would abort — the script
# handles the state-rm dance):
#   infra-down      — destroy everything BUT the data volume (kept, data intact)
#   infra-down-all  — destroy everything, snapshotting the volume first
infra-up:
	cd infra && terraform init && terraform apply

infra-down:
	./scripts/infra_teardown.sh keep

infra-down-all:
	./scripts/infra_teardown.sh all

# Build the portal image and push it to the ECR repo created by Terraform.
build-push:
	./scripts/build_push_portal.sh

# Seed the demo inventory into the real Databricks Unity Catalog (one-time).
seed-uc:
	set -a && . ./.env && set +a && python3 scripts/seed_catalog.py

# Seed the Cribl logging tier + edge collectors (run against the leader/edges,
# typically over an SSM port-forward — see README and cribl/README.md).
seed-cribl:
	python3 scripts/seed_cribl.py
