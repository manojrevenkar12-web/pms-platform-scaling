# Runbook: hand a CloudFormation stack over to Terraform

    aws cloudformation get-template --stack-name S --template-stage Processed > template.json
    aws cloudformation describe-stack-resources --stack-name S > resources.json
    aws cloudformation describe-stacks --stack-name S > stack.json
    cfn2tf -template template.json -resources resources.json -stack stack.json \
           -region eu-central-1 -account 123456789012 -out terraform/S

If it fails, the message lists every resource, property or function it cannot convert exactly.
Move those out of the stack first, or leave the stack in CloudFormation.

Then follow `terraform/S/HANDOVER.md`. The one rule: never apply a plan that adds, changes or
destroys anything. Stop at the first step whose output differs from what HANDOVER.md says.

Drift: if someone changed a resource by hand, the plan shows it as a change. Decide which side is
right, fix it through the stack (or by hand), and plan again until it is a pure import.
