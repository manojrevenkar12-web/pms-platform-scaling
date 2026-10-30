#!/usr/bin/env bash
# End-to-end handover against a local AWS (LocalStack): deploy the legacy stack with
# CloudFormation, generate Terraform with cfn2tf, and prove the handover changes nothing.
#
#   1. the plan is a pure import:   N to import, 0 to add, 0 to change, 0 to destroy
#   2. after apply:                  terraform plan shows no changes
#   3. after deleting the stack:     terraform plan still shows no changes (resources retained)
#
# Needs: aws CLI, terraform, go, and an endpoint at $AWS_ENDPOINT_URL (default localhost:4566).
set -Eeuo pipefail

endpoint="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=""
stack=legacy-integration
work="$(pwd)/build/handover"
rm -rf "${work:?}"
mkdir -p "$work"
aws_() { aws --endpoint-url "$endpoint" "$@"; }

# In GitHub Actions, failures are also reported as annotations (readable without the log).
log="$work/run.log"
annotate() { # level title file
  [ -n "${GITHUB_ACTIONS:-}" ] || return 0
  local body
  body=$(tail -n 60 "$3" | sed -e 's/%/%25/g' | sed -e ':a;N;$!ba;s/\n/%0A/g')
  echo "::$1 title=$2::$body"
}
trap 'rc=$?; if [ $rc -ne 0 ]; then sleep 1; annotate error "handover failed (exit $rc)" "$log"; fi' EXIT
exec > >(tee -a "$log") 2>&1

echo "== deploy the legacy stack with CloudFormation"
aws_ cloudformation deploy --stack-name "$stack" --template-file examples/legacy-stack/template.yaml \
  --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset
aws_ cloudformation describe-stack-resources --stack-name "$stack" > "$work/resources.json"
aws_ cloudformation describe-stacks --stack-name "$stack" > "$work/stack.json"
account=$(aws_ sts get-caller-identity --query Account --output text)

echo "== generate Terraform"
go run ./cmd/cfn2tf -template examples/legacy-stack/template.yaml -resources "$work/resources.json" \
  -stack "$work/stack.json" -region "$AWS_DEFAULT_REGION" -account "$account" -out "$work/tf"
cat > "$work/tf/localstack_override.tf" <<HCL
provider "aws" {
  region                      = "$AWS_DEFAULT_REGION"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true
  endpoints {
    cloudformation = "$endpoint"
    dynamodb       = "$endpoint"
    iam            = "$endpoint"
    logs           = "$endpoint"
    s3             = "$endpoint"
    sns            = "$endpoint"
    sqs            = "$endpoint"
    ssm            = "$endpoint"
    sts            = "$endpoint"
  }
}
HCL
expected=$(grep -c '^import {' "$work/tf/imports.tf")

echo "== step 1: retain every resource"
aws_ cloudformation update-stack --stack-name "$stack" --template-body "file://$work/tf/retain-template.json" \
  --parameters ParameterKey=Environment,UsePreviousValue=true ParameterKey=ExportRetentionDays,UsePreviousValue=true \
  --capabilities CAPABILITY_NAMED_IAM
aws_ cloudformation wait stack-update-complete --stack-name "$stack"

cd "$work/tf"
terraform fmt -check -diff > "$work/fmt.diff" || echo "note: terraform fmt would reformat (see build/handover/fmt.diff)"
terraform init -input=false -no-color > /dev/null
terraform validate -no-color

echo "== step 2: the plan must be a pure import"
terraform plan -input=false -no-color -out handover.plan | tee "$work/plan.txt"
want="Plan: $expected to import, 0 to add, 0 to change, 0 to destroy."
grep -E '^(Plan:|  # |      [~+-] )' "$work/plan.txt" > "$work/plan-summary.txt" || true
annotate notice "plan" "$work/plan-summary.txt"
grep -qF "$want" "$work/plan.txt" || { echo "FAIL: expected '$want'"; exit 1; }

echo "== step 3: import, then no changes"
terraform apply -input=false -no-color handover.plan > "$work/apply.txt"
terraform plan -input=false -no-color -detailed-exitcode > "$work/plan-after-apply.txt" || {
  cat "$work/plan-after-apply.txt"; echo "FAIL: changes after import"; exit 1; }

echo "== step 4: delete the stack; resources stay, still no changes"
aws_ cloudformation delete-stack --stack-name "$stack"
aws_ cloudformation wait stack-delete-complete --stack-name "$stack"
terraform plan -input=false -no-color -refresh=true -detailed-exitcode > "$work/plan-after-delete.txt" || {
  cat "$work/plan-after-delete.txt"; echo "FAIL: changes after deleting the stack"; exit 1; }

cat > "$work/result.json" <<JSON
{"imports": $expected, "plan": "$want", "after_apply": "no changes", "after_stack_delete": "no changes"}
JSON
echo "PASS: $expected resources handed over; no changes after import or after deleting the stack" > "$work/pass.txt"
annotate notice "handover passed" "$work/pass.txt"
echo "PASS: $expected resources handed over; no changes after import or after deleting the stack"
