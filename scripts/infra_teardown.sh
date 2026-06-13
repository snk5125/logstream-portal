#!/usr/bin/env bash
# Tear down the LogStream Portal stack. The portal data EBS volume carries
# `lifecycle { prevent_destroy = true }`, which makes a plain `terraform destroy`
# abort and delete NOTHING. This script removes the volume (and its attachment)
# from Terraform state first so the destroy can proceed, then branches:
#
#   keep (default) — destroy everything else; leave the EBS volume in AWS
#                    (status `available`, data intact). Prints the re-import
#                    command for the next `make infra-up`.
#   all            — also delete the volume; snapshots it first (recoverable)
#                    unless --no-snapshot is passed.
#
# Run this from a directory whose ./infra holds the live terraform state.
set -euo pipefail
cd "$(dirname "$0")/../infra"

MODE="${1:-keep}"
REGION="${AWS_REGION:-us-east-1}"
VOL_ADDR="module.compute.aws_ebs_volume.portal_data"
ATT_ADDR="module.compute.aws_volume_attachment.portal_data"

if [ "$MODE" != keep ] && [ "$MODE" != all ]; then
  echo "usage: infra_teardown.sh [keep|all] [--no-snapshot]" >&2
  exit 2
fi

VOL="$(terraform output -raw portal_data_volume_id 2>/dev/null || true)"
if [ -z "$VOL" ]; then
  # Output may predate this state; read the id straight from the resource.
  VOL="$(terraform state show "$VOL_ADDR" 2>/dev/null \
    | sed -n 's/^[[:space:]]*id[[:space:]]*=[[:space:]]*"\(vol-[^"]*\)".*/\1/p' | head -1)"
fi

# Single up-front confirmation — so we never mutate state and then abort mid-run.
echo "About to tear down the stack (mode: $MODE)."
[ "$MODE" = all ] && echo "  → the data volume ${VOL:-<none>} WILL be deleted (snapshot first unless --no-snapshot)."
[ "$MODE" = keep ] && echo "  → the data volume ${VOL:-<none>} will be KEPT."
read -r -p "Type 'destroy' to proceed: " ANS
[ "$ANS" = destroy ] || { echo "aborted; nothing changed."; exit 1; }

# Detach the volume from Terraform management so prevent_destroy doesn't block.
if [ -n "$VOL" ]; then
  terraform state rm "$ATT_ADDR" "$VOL_ADDR" >/dev/null
  echo "removed $VOL (and its attachment) from terraform state"
fi

terraform destroy -auto-approve

if [ -z "$VOL" ]; then
  echo "no portal data volume was in state; full teardown complete."
  exit 0
fi

if [ "$MODE" = all ]; then
  if [ "${2:-}" != "--no-snapshot" ]; then
    SNAP="$(aws ec2 create-snapshot --volume-id "$VOL" \
      --description "logstream-portal-data pre-delete" \
      --region "$REGION" --query SnapshotId --output text)"
    echo "snapshot $SNAP created (recover: aws ec2 create-volume --snapshot-id $SNAP --availability-zone <az>)"
    aws ec2 wait snapshot-completed --snapshot-ids "$SNAP" --region "$REGION"
  fi
  aws ec2 delete-volume --volume-id "$VOL" --region "$REGION"
  echo "deleted data volume $VOL — teardown complete, spend at ~zero."
else
  echo
  echo "KEPT data volume $VOL (status: available, data intact)."
  echo "Before the next 'make infra-up', re-import it so a fresh empty volume isn't created:"
  echo "  cd infra && terraform import $VOL_ADDR $VOL"
fi
