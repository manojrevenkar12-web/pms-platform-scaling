# 3. Hand CloudFormation stacks over to Terraform by import, never by recreation

Status: accepted

## Context

Some infrastructure was created years ago with CloudFormation; new infrastructure is Terraform.
Recreating live queues, buckets and tables is not an option (messages, objects and items would be
lost, and their names and ARNs are wired into other systems).

## Decision

`cfn2tf` reads the deployed template, the stack's resources (physical IDs) and its parameter
values, and writes Terraform that describes exactly what exists, with an `import` block per
resource and a handover plan:

1. update the stack so every resource has `DeletionPolicy` and `UpdateReplacePolicy` Retain;
2. `terraform plan` must be a pure import: N to import, 0 to add, 0 to change, 0 to destroy;
3. apply, then plan again: no changes;
4. delete the stack; plan again: still no changes.

Rules that keep it honest:

- **Refuse rather than guess.** An unsupported resource type, property or intrinsic function is
  an error that names the logical ID. A silently dropped property would show up as a diff at best
  and as a changed resource at worst.
- **Resolve what CloudFormation resolved.** Conditions, mappings and pseudo parameters are
  evaluated with the deployed values; parameters become variables defaulting to them.
- **Keep CloudFormation's values exactly.** Generated names are taken from the physical IDs (a
  rename would replace the resource), and attributes that differ between the two (a log group's
  ARN ends in `:*` in CloudFormation) are reproduced.
- **Judge the plan, not the summary line.** `cfn2tf -check-plan` reads `terraform show -json`
  and lists every resource that would really change. An update that only rewrites a JSON
  document with the same meaning (a policy returned with other whitespace) is reported apart.
  Two SNS subscription fields that exist only in Terraform are ignored in the generated code,
  because an import leaves them unset and every subscription would otherwise plan an update.
- **Proved in CI** against LocalStack: the real CloudFormation deploy, the real Terraform AWS
  provider, steps 1 to 3 (scripts/handover-e2e.sh).

## What LocalStack does not show

LocalStack 4.4's CloudFormation leaves out three things AWS does: S3 lifecycle rules, log group
retention, and `DeletionPolicy` when a stack is deleted. The first run of the e2e found the first
two as differences in the plan, which is the handover working as intended: the plan catches a
deployed state that differs from the template. The e2e now applies those two through the service
APIs, as the template says. Step 4 (deleting the stack) is not run on LocalStack, because it would
delete the retained resources; a unit test checks that the retain template retains every resource,
and the e2e checks that the stack accepted it.

Open check for the real account: CloudFormation's lifecycle `Prefix` is written as a filter
prefix. If a bucket was configured with the legacy top-level prefix, the plan shows it, and the
generated rule needs `prefix` instead of `filter`.

Supported today: S3 buckets (versioning, encryption, lifecycle, public access block) and bucket
policies, SQS queues and queue policies, SNS topics and subscriptions, IAM roles, inline and
managed policies, CloudWatch log groups, SSM parameters, DynamoDB tables.

## Consequences

- A stack with anything else is converted after that part is moved by hand, or left in
  CloudFormation; the tool's error message is the to-do list.
- LocalStack is not AWS. Before a production handover, run step 2 against the real account: the
  plan is the proof, and it costs nothing to run.
