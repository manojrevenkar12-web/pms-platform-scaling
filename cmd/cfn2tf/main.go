// Command cfn2tf hands a deployed CloudFormation stack over to Terraform.
//
//	aws cloudformation describe-stack-resources --stack-name S > resources.json
//	aws cloudformation describe-stacks --stack-name S > stack.json
//	cfn2tf -template template.yaml -resources resources.json -stack stack.json \
//	       -region eu-central-1 -account 123456789012 -out terraform/
//
// It writes main.tf, imports.tf, variables.tf, outputs.tf, versions.tf, retain-template.json and
// HANDOVER.md, or fails with the list of everything it cannot convert exactly.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cfn"
	"github.com/manojrevenkar12-web/pms-platform-scaling/internal/cfn2tf"
)

func main() {
	tmpl := flag.String("template", "", "the stack's template (YAML or JSON); use the processed template if it had transforms")
	resources := flag.String("resources", "", "output of aws cloudformation describe-stack-resources")
	stack := flag.String("stack", "", "output of aws cloudformation describe-stacks (for parameter values)")
	region := flag.String("region", os.Getenv("AWS_REGION"), "region of the stack")
	account := flag.String("account", "", "account ID of the stack")
	partition := flag.String("partition", "aws", "AWS partition")
	out := flag.String("out", "terraform", "output directory")
	flag.Parse()
	if err := run(*tmpl, *resources, *stack, *region, *account, *partition, *out); err != nil {
		fmt.Fprintln(os.Stderr, "cfn2tf:", err)
		os.Exit(1)
	}
}

func run(tmplPath, resPath, stackPath, region, account, partition, out string) error {
	if tmplPath == "" || resPath == "" || stackPath == "" || region == "" || account == "" {
		return errors.New("-template, -resources, -stack, -region and -account are required")
	}
	b, err := os.ReadFile(tmplPath)
	if err != nil {
		return err
	}
	t, err := cfn.Parse(b)
	if err != nil {
		return err
	}
	var res cfn.StackResources
	if err := readJSON(resPath, &res); err != nil {
		return err
	}
	var st cfn.Stacks
	if err := readJSON(stackPath, &st); err != nil {
		return err
	}
	if len(st.Stacks) != 1 {
		return fmt.Errorf("%s: expected exactly one stack, found %d", stackPath, len(st.Stacks))
	}
	in := cfn2tf.Input{Template: t, StackName: st.Stacks[0].StackName, Region: region, AccountID: account, Partition: partition,
		Params: map[string]string{}, Physical: map[string]string{}}
	for _, p := range st.Stacks[0].Parameters {
		in.Params[p.ParameterKey] = p.ParameterValue
	}
	for _, r := range res.StackResources {
		in.Physical[r.LogicalResourceID] = r.PhysicalResourceID
	}
	o, err := cfn2tf.Convert(in)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	names := make([]string, 0, len(o.Files))
	for n := range o.Files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(out, n), []byte(o.Files[n]), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("wrote %d files to %s: %d resources, %d imports\n", len(names), out, len(o.Resources), o.Imports)
	return nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
