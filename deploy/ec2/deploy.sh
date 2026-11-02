#!/usr/bin/env bash
# Uploads a release tarball and deploys deploy/ec2/cloudformation.yaml.
#
#   deploy/ec2/deploy.sh --stack flowd --bucket my-artifacts \
#       --tarball dist/flowd-0.1.0-aarch64.tar.gz \
#       --vpc vpc-0123 --subnets subnet-a,subnet-b [--region eu-west-1] [--yes]
#
# Without --yes the script only prints the commands it would run. With --yes
# it CREATES BILLABLE AWS RESOURCES (DynamoDB table, EC2 instances, ...).
# Requires the AWS CLI v2 with credentials for the target account.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK="" BUCKET="" TARBALL="" VPC="" SUBNETS="" REGION="${AWS_REGION:-}" APPLY=0
EXTRA=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stack) STACK="$2"; shift 2 ;;
        --bucket) BUCKET="$2"; shift 2 ;;
        --tarball) TARBALL="$2"; shift 2 ;;
        --vpc) VPC="$2"; shift 2 ;;
        --subnets) SUBNETS="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --param) EXTRA+=("$2"); shift 2 ;;
        --yes) APPLY=1; shift ;;
        *) echo "unknown argument $1" >&2; exit 2 ;;
    esac
done
for v in STACK BUCKET TARBALL VPC SUBNETS; do
    [[ -n "${!v}" ]] || { echo "missing --${v,,}" >&2; exit 2; }
done
[[ -f "$TARBALL" ]] || { echo "no such tarball: $TARBALL" >&2; exit 2; }

KEY="releases/$(basename "$TARBALL")"
REGION_ARGS=()
[[ -n "$REGION" ]] && REGION_ARGS=(--region "$REGION")

run() {
    if ((APPLY)); then
        "$@"
    else
        printf 'would run:'
        printf ' %q' "$@"
        printf '\n'
    fi
}

run aws "${REGION_ARGS[@]}" s3 cp "$TARBALL" "s3://$BUCKET/$KEY"
run aws "${REGION_ARGS[@]}" cloudformation deploy \
    --stack-name "$STACK" \
    --template-file "$HERE/cloudformation.yaml" \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides "VpcId=$VPC" "SubnetIds=$SUBNETS" \
    "ArtifactBucket=$BUCKET" "ArtifactKey=$KEY" "${EXTRA[@]}"

if ((!APPLY)); then
    echo
    echo "Dry run only. Re-run with --yes to create the (billable) resources."
fi
