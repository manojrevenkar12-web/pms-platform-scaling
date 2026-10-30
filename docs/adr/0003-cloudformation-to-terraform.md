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
- **Proved in CI** against LocalStack: the real CloudFormation deploy, the real Terraform AWS
  provider, the four steps above (scripts/handover-e2e.sh).

Supported today: S3 buckets (versioning, encryption, lifecycle, public access block) and bucket
policies, SQS queues and queue policies, SNS topics and subscriptions, IAM roles, inline and
managed policies, CloudWatch log groups, SSM parameters, DynamoDB tables.

## Consequences

- A stack with anything else is converted after that part is moved by hand, or left in
  CloudFormation; the tool's error message is the to-do list.
- LocalStack is not AWS. Before a production handover, run step 2 against the real account: the
  plan is the proof, and it costs nothing to run.
