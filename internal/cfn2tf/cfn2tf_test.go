package cfn2tf

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cfn"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func exampleInput(t *testing.T) Input {
	t.Helper()
	b, err := os.ReadFile("../../examples/legacy-stack/template.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := cfn.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	var res cfn.StackResources
	var st cfn.Stacks
	readJSON(t, "testdata/resources.json", &res)
	readJSON(t, "testdata/stack.json", &st)
	in := Input{Template: tmpl, StackName: st.Stacks[0].StackName, Region: "eu-central-1", AccountID: "123456789012", Params: map[string]string{}, Physical: map[string]string{}}
	for _, p := range st.Stacks[0].Parameters {
		in.Params[p.ParameterKey] = p.ParameterValue
	}
	for _, r := range res.StackResources {
		in.Physical[r.LogicalResourceID] = r.PhysicalResourceID
	}
	return in
}

func readJSON(t *testing.T, path string, v any) {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// The example stack converts to the reviewed Terraform in testdata/golden.
func TestGolden(t *testing.T) {
	out, err := Convert(exampleInput(t))
	if err != nil {
		t.Fatal(err)
	}
	if out.Imports != 17 || len(out.Resources) != 17 {
		t.Fatalf("%d imports for %d resources, want 17 and 17", out.Imports, len(out.Resources))
	}
	for name, content := range out.Files {
		path := filepath.Join("testdata", "golden", name)
		if *update {
			_ = os.MkdirAll(filepath.Dir(path), 0o755)
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v (run go test ./internal/cfn2tf -update)", name, err)
		}
		if string(want) != content {
			t.Errorf("%s differs from the golden file (run with -update after reviewing)", name)
		}
	}
}

func TestEveryResourceIsImportedOnce(t *testing.T) {
	out, err := Convert(exampleInput(t))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, line := range strings.Split(out.Files["imports.tf"], "\n") {
		if to, ok := strings.CutPrefix(strings.TrimSpace(line), "to = "); ok {
			seen[to]++
		}
	}
	for _, r := range out.Resources {
		if seen[r] != 1 {
			t.Errorf("%s imported %d times", r, seen[r])
		}
	}
}

func TestResolvedWithDeployedValues(t *testing.T) {
	in := exampleInput(t)
	out, err := Convert(in)
	if err != nil {
		t.Fatal(err)
	}
	main := out.Files["main.tf"]
	for _, want := range []string{
		`retention_in_days = 14`,                               // FindInMap with Environment = dev
		`id     = "archive-audit"`,                             // Fn::If on a condition that is true in dev
		`"${aws_cloudwatch_log_group.worker_log_group.arn}:*"`, // CloudFormation's log group ARN ends in :*
		`days = var.export_retention_days`,
	} {
		if !strings.Contains(main, want) {
			t.Errorf("main.tf lacks %q", want)
		}
	}
	in.Params["Environment"] = "prod"
	out, err = Convert(in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Files["main.tf"], "archive-audit") || !strings.Contains(out.Files["main.tf"], "retention_in_days = 90") {
		t.Error("prod parameters were not applied")
	}
}

func convertYAML(t *testing.T, y string, physical map[string]string) error {
	t.Helper()
	tmpl, err := cfn.Parse([]byte(y))
	if err != nil {
		return err
	}
	_, err = Convert(Input{Template: tmpl, StackName: "s", Region: "eu-central-1", AccountID: "1", Physical: physical})
	return err
}

func TestRefusals(t *testing.T) {
	cases := map[string]struct {
		yaml, want string
	}{
		"unknown type":            {`Resources: {Fn: {Type: AWS::Lambda::Function, Properties: {}}}`, "AWS::Lambda::Function is not supported"},
		"unknown property":        {`Resources: {Q: {Type: AWS::SQS::Queue, Properties: {QueueName: q, Bogus: 1}}}`, "Q.Bogus: property is not supported"},
		"nested unknown property": {`Resources: {B: {Type: AWS::S3::Bucket, Properties: {VersioningConfiguration: {Status: Enabled, MfaDelete: x}}}}`, "B.VersioningConfiguration.MfaDelete"},
		"import value":            {`Resources: {P: {Type: AWS::SSM::Parameter, Properties: {Type: String, Value: !ImportValue other}}}`, "Fn::ImportValue is not supported"},
		"inline subscription":     {`Resources: {T: {Type: AWS::SNS::Topic, Properties: {Subscription: [{Protocol: sqs, Endpoint: x}]}}}`, "inline subscriptions"},
		"not deployed":            {`Resources: {Q2: {Type: AWS::SQS::Queue}}`, "Q2: not in the stack"},
		"dangling ref":            {`Resources: {P: {Type: AWS::SSM::Parameter, Properties: {Type: String, Value: !Ref Nope}}}`, "Ref Nope points at nothing"},
	}
	phys := map[string]string{"Fn": "x", "Q": "https://sqs/1/q", "B": "b", "P": "/p", "T": "arn:aws:sns:r:1:t"}
	for name, tc := range cases {
		err := convertYAML(t, tc.yaml, phys)
		var u *Unsupported
		if !errors.As(err, &u) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an Unsupported error mentioning %q", name, err, tc.want)
		}
	}
	for name, y := range map[string]string{
		"transform": "Transform: AWS::Serverless-2016-10-31\nResources: {}",
		"export":    "Resources: {}\nOutputs: {X: {Value: a, Export: {Name: x}}}",
	} {
		if _, err := cfn.Parse([]byte(y)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}

func TestSkippedByCondition(t *testing.T) {
	y := `
Parameters: {Env: {Type: String, Default: dev}}
Conditions: {Prod: !Equals [!Ref Env, prod]}
Resources:
  Q: {Type: AWS::SQS::Queue, Condition: Prod, Properties: {QueueName: q}}
  P: {Type: AWS::SSM::Parameter, Properties: {Type: String, Value: x, Description: !If [Prod, d, !Ref AWS::NoValue]}}
`
	tmpl, err := cfn.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Convert(Input{Template: tmpl, StackName: "s", Region: "r", AccountID: "1", Physical: map[string]string{"P": "/p"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Files["main.tf"], "aws_sqs_queue") || strings.Contains(out.Files["main.tf"], "description") {
		t.Fatalf("condition not applied:\n%s", out.Files["main.tf"])
	}
}

func TestEscaping(t *testing.T) {
	got := Render([]*Block{(&Block{Type: "resource", Labels: []string{"t", "n"}}).Set("v", "a \"q\" ${not} %{nor} \\ end")})
	want := `v = "a \"q\" $${not} %%{nor} \\ end"`
	if !strings.Contains(got, want) {
		t.Fatalf("got %s want %s", got, want)
	}
	sub := Template{"arn:", Raw("x.y"), ":${literal}"}
	if e := expr(sub, 0); e != `"arn:${x.y}:$${literal}"` {
		t.Fatalf("template: %s", e)
	}
}

func TestSnake(t *testing.T) {
	for in, want := range map[string]string{"ExportsBucket": "exports_bucket", "SQSQueue": "sqs_queue", "MyS3Bucket2": "my_s3_bucket2", "a-b": "a_b", "9x": "r_9x"} {
		if got := snake(in); got != want {
			t.Errorf("snake(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestSubEscapes(t *testing.T) {
	y := `Resources: {P: {Type: AWS::SSM::Parameter, Properties: {Type: String, Value: !Sub "${!Literal}-${AWS::Region}-${Q.Arn}", Name: !Join ["/", [a, !Ref AWS::AccountId]]}}, Q: {Type: AWS::SQS::Queue}}`
	tmpl, _ := cfn.Parse([]byte(y))
	out, err := Convert(Input{Template: tmpl, StackName: "s", Region: "r", AccountID: "1", Physical: map[string]string{"P": "/p", "Q": "https://q/1/q"}})
	if err != nil {
		t.Fatal(err)
	}
	m := out.Files["main.tf"]
	for _, want := range []string{`"$${Literal}-${data.aws_region.current.name}-${aws_sqs_queue.q.arn}"`, `"a/${data.aws_caller_identity.current.account_id}"`} {
		if !strings.Contains(m, want) {
			t.Errorf("missing %s in\n%s", want, m)
		}
	}
}
