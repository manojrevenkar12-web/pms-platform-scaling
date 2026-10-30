// Package cfn reads CloudFormation templates (YAML or JSON, including the short-form tags such as
// !Ref, !Sub and !GetAtt) into plain values. Intrinsic functions become one-key maps exactly as
// in the JSON form: {"Ref": "X"}, {"Fn::GetAtt": ["X", "Arn"]}, {"Fn::Sub": "..."}.
package cfn

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Template is a parsed template. Resources keep the order of the file.
type Template struct {
	Description string
	Parameters  map[string]Parameter
	ParamOrder  []string
	Mappings    map[string]any
	Conditions  map[string]any
	Resources   []Resource
	Outputs     []Output
	// Raw is the whole document with intrinsics in their JSON form, for writing it back.
	Raw map[string]any
}

// Parameter is a template parameter.
type Parameter struct {
	Name          string
	Type          string
	Default       any
	Description   string
	NoEcho        bool
	AllowedValues []any
}

// Resource is one resource.
type Resource struct {
	LogicalID           string
	Type                string
	Properties          map[string]any
	Condition           string
	DependsOn           []string
	DeletionPolicy      string
	UpdateReplacePolicy string
}

// Output is one stack output.
type Output struct {
	Name        string
	Description string
	Value       any
	Condition   string
}

// Parse reads a template.
func Parse(b []byte) (*Template, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("template: top level must be a mapping")
	}
	doc := root.Content[0]
	t := &Template{Parameters: map[string]Parameter{}, Mappings: map[string]any{}, Conditions: map[string]any{}}
	raw, err := toValue(doc)
	if err != nil {
		return nil, err
	}
	t.Raw, _ = raw.(map[string]any)
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key, val := doc.Content[i].Value, doc.Content[i+1]
		switch key {
		case "AWSTemplateFormatVersion", "Metadata", "Rules":
		case "Transform":
			return nil, fmt.Errorf("template uses Transform (%s); expand it first (aws cloudformation get-template --template-stage Processed)", nodeString(val))
		case "Description":
			t.Description = val.Value
		case "Parameters":
			for j := 0; j+1 < len(val.Content); j += 2 {
				name := val.Content[j].Value
				raw, err := toValue(val.Content[j+1])
				if err != nil {
					return nil, err
				}
				m, _ := raw.(map[string]any)
				p := Parameter{Name: name}
				p.Type, _ = m["Type"].(string)
				p.Default = m["Default"]
				p.Description, _ = m["Description"].(string)
				p.NoEcho = truthy(m["NoEcho"])
				p.AllowedValues, _ = m["AllowedValues"].([]any)
				t.Parameters[name] = p
				t.ParamOrder = append(t.ParamOrder, name)
			}
		case "Mappings", "Conditions":
			v, err := toValue(val)
			if err != nil {
				return nil, err
			}
			m, _ := v.(map[string]any)
			if key == "Mappings" {
				t.Mappings = m
			} else {
				t.Conditions = m
			}
		case "Resources":
			for j := 0; j+1 < len(val.Content); j += 2 {
				id := val.Content[j].Value
				raw, err := toValue(val.Content[j+1])
				if err != nil {
					return nil, fmt.Errorf("resource %s: %w", id, err)
				}
				m, ok := raw.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("resource %s is not a mapping", id)
				}
				r := Resource{LogicalID: id}
				r.Type, _ = m["Type"].(string)
				r.Properties, _ = m["Properties"].(map[string]any)
				if r.Properties == nil {
					r.Properties = map[string]any{}
				}
				r.Condition, _ = m["Condition"].(string)
				r.DeletionPolicy, _ = m["DeletionPolicy"].(string)
				r.UpdateReplacePolicy, _ = m["UpdateReplacePolicy"].(string)
				switch d := m["DependsOn"].(type) {
				case string:
					r.DependsOn = []string{d}
				case []any:
					for _, x := range d {
						if s, ok := x.(string); ok {
							r.DependsOn = append(r.DependsOn, s)
						}
					}
				}
				if r.Type == "" {
					return nil, fmt.Errorf("resource %s has no Type", id)
				}
				t.Resources = append(t.Resources, r)
			}
		case "Outputs":
			for j := 0; j+1 < len(val.Content); j += 2 {
				raw, err := toValue(val.Content[j+1])
				if err != nil {
					return nil, err
				}
				m, _ := raw.(map[string]any)
				o := Output{Name: val.Content[j].Value, Value: m["Value"]}
				o.Description, _ = m["Description"].(string)
				o.Condition, _ = m["Condition"].(string)
				if _, ok := m["Export"]; ok {
					return nil, fmt.Errorf("output %s is exported; other stacks import it, so move them first or remove the export", o.Name)
				}
				t.Outputs = append(t.Outputs, o)
			}
		default:
			return nil, fmt.Errorf("template: unknown top-level section %q", key)
		}
	}
	return t, nil
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(x, "true")
	}
	return false
}

func nodeString(n *yaml.Node) string {
	if n.Kind == yaml.ScalarNode {
		return n.Value
	}
	v, _ := toValue(n)
	return fmt.Sprint(v)
}

// toValue converts a node, turning short-form tags into intrinsic maps.
func toValue(n *yaml.Node) (any, error) {
	v, err := plain(n)
	if err != nil {
		return nil, err
	}
	tag := n.Tag
	if !strings.HasPrefix(tag, "!") || strings.HasPrefix(tag, "!!") {
		return v, nil
	}
	name := tag[1:]
	switch name {
	case "Ref", "Condition":
		return map[string]any{name: v}, nil
	case "GetAtt":
		if s, ok := v.(string); ok {
			res, attr, found := strings.Cut(s, ".")
			if !found {
				return nil, fmt.Errorf("line %d: !GetAtt %q needs Resource.Attribute", n.Line, s)
			}
			return map[string]any{"Fn::GetAtt": []any{res, attr}}, nil
		}
		return map[string]any{"Fn::GetAtt": v}, nil
	default:
		return map[string]any{"Fn::" + name: v}, nil
	}
}

func plain(n *yaml.Node) (any, error) {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return toValue(n.Content[0])
	case yaml.AliasNode:
		return toValue(n.Alias)
	case yaml.ScalarNode:
		return scalar(n), nil
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := toValue(c)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case yaml.MappingNode:
		out := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			v, err := toValue(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			out[n.Content[i].Value] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("line %d: unsupported YAML node", n.Line)
}

// scalar keeps CloudFormation's view: numbers and booleans stay typed when the YAML says so.
func scalar(n *yaml.Node) any {
	switch n.ShortTag() {
	case "!!int":
		if i, err := strconv.ParseInt(n.Value, 0, 64); err == nil {
			return i
		}
	case "!!float":
		if f, err := strconv.ParseFloat(n.Value, 64); err == nil {
			return f
		}
	case "!!bool":
		return strings.EqualFold(n.Value, "true")
	case "!!null":
		return nil
	}
	return n.Value
}

// StackResources is the output of `aws cloudformation describe-stack-resources`.
type StackResources struct {
	StackResources []struct {
		LogicalResourceID  string `json:"LogicalResourceId"`
		PhysicalResourceID string `json:"PhysicalResourceId"`
		ResourceType       string `json:"ResourceType"`
		ResourceStatus     string `json:"ResourceStatus"`
	} `json:"StackResources"`
}

// Stacks is the output of `aws cloudformation describe-stacks`.
type Stacks struct {
	Stacks []struct {
		StackName   string `json:"StackName"`
		StackID     string `json:"StackId"`
		StackStatus string `json:"StackStatus"`
		Parameters  []struct {
			ParameterKey   string `json:"ParameterKey"`
			ParameterValue string `json:"ParameterValue"`
		} `json:"Parameters"`
	} `json:"Stacks"`
}
