#!/usr/bin/env bash
# End-to-end handover against a local AWS (LocalStack): deploy the legacy stack with
# CloudFormation, generate Terraform with cfn2tf, and prove the handover changes nothing.
#
#   1. the stack accepts the retain template (every resource DeletionPolicy: Retain)
#   2. the plan only imports: every resource imported, no real change (cfn2tf -check-plan)
#   3. after apply: terraform plan shows no changes
#
# LocalStack 4.4's CloudFormation does not apply three things that AWS does (see the
# resource providers in localstack-core 4.4.0): S3 LifecycleConfiguration, log group
# RetentionInDays, and DeletionPolicy on stack deletion. The first two are applied below through
# the service APIs, exactly as the template says, so the deployed state is what CloudFormation
# makes on AWS. Stack deletion (handover step 4) is not run here, because LocalStack would delete
# the retained resources; the unit tests check that the retain template retains every resource.
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
  body=$(tail -n 150 "$3" | sed -e 's/%/%25/g' | sed -e ':a;N;$!ba;s/\n/%0A/g')
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
bucket=$(python3 -c 'import json,sys; print([r["PhysicalResourceId"] for r in json.load(open(sys.argv[1]))["StackResources"] if r["LogicalResourceId"]=="ExportsBucket"][0])' "$work/resources.json")
for call in get-bucket-versioning get-bucket-encryption get-bucket-lifecycle-configuration get-public-access-block; do
  echo "s3api $call: $(aws_ s3api $call --bucket "$bucket" --output json 2>&1 | tr -d '\n' | cut -c1-400)"
done > "$work/bucket.txt"
annotate notice "bucket as deployed" "$work/bucket.txt"

echo "== fill LocalStack's CloudFormation gaps (lifecycle rules, log retention), as the template says"
cat > "$work/lifecycle.json" <<JSON
{"Rules": [
  {"ID": "expire-exports", "Status": "Enabled", "Filter": {"Prefix": "exports/"},
   "Expiration": {"Days": 90}, "NoncurrentVersionExpiration": {"NoncurrentDays": 7}},
  {"ID": "archive-audit", "Status": "Enabled", "Filter": {"Prefix": "audit/"},
   "Transitions": [{"Days": 30, "StorageClass": "GLACIER"}]}
]}
JSON
aws_ s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration "file://$work/lifecycle.json"
aws_ logs put-retention-policy --log-group-name /pms/dev/channel-sync --retention-in-days 14

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

echo "== step 2: the plan must only import"
set +e
terraform plan -input=false -no-color -out handover.plan > "$work/plan.txt" 2>&1
plan_rc=$?
set -e
grep -E 'will be updated|must be replaced|will be created|will be destroyed|^ +[~+-] |^Plan:|^Error' "$work/plan.txt" | grep -v '^ *+ .*= (known after apply)' > "$work/plan-changes.txt" || true
[ $plan_rc -eq 0 ] || { annotate warning "plan" "$work/plan-changes.txt"; tail -n 30 "$work/plan.txt"; exit 1; }
summary=$(grep '^Plan:' "$work/plan.txt" || echo "Plan: no changes")
terraform show -json handover.plan > "$work/plan.json"
go -C "$OLDPWD" run ./cmd/cfn2tf -check-plan "$work/plan.json" | tee "$work/check.txt"
grep -q "^$expected to import, 0 real changes" "$work/check.txt" || { echo "FAIL: expected $expected imports and no real change"; exit 1; }
{ echo "$summary"; cat "$work/check.txt"; } > "$work/plan-check.txt"
annotate notice "plan check" "$work/plan-check.txt"

echo "== step 3: import, then no changes"
terraform apply -input=false -no-color handover.plan > "$work/apply.txt"
terraform plan -input=false -no-color -detailed-exitcode > "$work/plan-after-apply.txt" || {
  cat "$work/plan-after-apply.txt"; echo "FAIL: changes after import"; exit 1; }

echo "== step 1, checked: the stack now retains every resource"
aws_ cloudformation get-template --stack-name "$stack" --query TemplateBody --output json > "$work/deployed-template.json"
python3 - "$work/deployed-template.json" <<'PY'
import json, sys
t = json.load(open(sys.argv[1]))
if isinstance(t, str):
    t = json.loads(t)
bad = [k for k, r in t["Resources"].items() if r.get("DeletionPolicy") != "Retain" or r.get("UpdateReplacePolicy") != "Retain"]
print(f"{len(t['Resources'])} resources, not retained: {bad}")
sys.exit(1 if bad else 0)
PY

cat > "$work/result.json" <<JSON
{"imports": $expected, "plan": "$summary", "real_changes": 0, "after_apply": "no changes", "stack_retains_all": true}
JSON
echo "PASS: $expected resources imported with no real change; no changes after apply; the stack retains every resource" > "$work/pass.txt"
annotate notice "handover passed" "$work/pass.txt"
cat "$work/pass.txt"
