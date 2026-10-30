package cfn2tf

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// PlanReport classifies a Terraform plan (terraform show -json) for a handover.
type PlanReport struct {
	Imports    int      `json:"imports"`
	Equivalent []string `json:"equivalent"` // updates that rewrite a JSON document with the same meaning
	Changes    []string `json:"changes"`    // anything that would really change, create or destroy
}

// OK reports whether the plan only imports.
func (r PlanReport) OK() bool { return len(r.Changes) == 0 }

func (r PlanReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d to import, %d real changes, %d equivalent JSON rewrites\n", r.Imports, len(r.Changes), len(r.Equivalent))
	for _, c := range r.Changes {
		b.WriteString("  change: " + c + "\n")
	}
	for _, e := range r.Equivalent {
		b.WriteString("  equivalent: " + e + "\n")
	}
	return b.String()
}

type planJSON struct {
	ResourceChanges []struct {
		Address string `json:"address"`
		Change  struct {
			Actions      []string       `json:"actions"`
			Before       map[string]any `json:"before"`
			After        map[string]any `json:"after"`
			AfterUnknown map[string]any `json:"after_unknown"`
			Importing    *struct {
				ID string `json:"id"`
			} `json:"importing"`
		} `json:"change"`
	} `json:"resource_changes"`
}

// CheckPlan reads `terraform show -json <plan>` and lists every change that is not an import.
// An update whose only differences are JSON strings with the same meaning (a policy returned
// with different whitespace or key order) is reported as equivalent rather than as a change.
func CheckPlan(b []byte) (PlanReport, error) {
	var p planJSON
	if err := json.Unmarshal(b, &p); err != nil {
		return PlanReport{}, fmt.Errorf("plan JSON: %w", err)
	}
	var r PlanReport
	for _, rc := range p.ResourceChanges {
		if strings.HasPrefix(rc.Address, "data.") {
			continue
		}
		if rc.Change.Importing != nil {
			r.Imports++
		}
		actions := strings.Join(rc.Change.Actions, ",")
		switch actions {
		case "no-op", "read":
			continue
		case "update":
			changed := changedKeys(rc.Change.Before, rc.Change.After, rc.Change.AfterUnknown)
			var real, same []string
			for _, k := range changed {
				if sameJSON(rc.Change.Before[k], rc.Change.After[k]) {
					same = append(same, k)
				} else {
					real = append(real, k)
				}
			}
			if len(real) == 0 {
				r.Equivalent = append(r.Equivalent, rc.Address+" ("+strings.Join(same, ", ")+")")
				continue
			}
			r.Changes = append(r.Changes, rc.Address+" update: "+strings.Join(real, ", "))
		default:
			r.Changes = append(r.Changes, rc.Address+" "+actions)
		}
	}
	return r, nil
}

func changedKeys(before, after, unknown map[string]any) []string {
	keys := map[string]bool{}
	for k := range before {
		keys[k] = true
	}
	for k := range after {
		keys[k] = true
	}
	var out []string
	for k := range keys {
		if u, ok := unknown[k].(bool); ok && u {
			out = append(out, k)
			continue
		}
		if !reflect.DeepEqual(before[k], after[k]) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func sameJSON(a, b any) bool {
	as, ok1 := a.(string)
	bs, ok2 := b.(string)
	if !ok1 || !ok2 {
		return false
	}
	var av, bv any
	if json.Unmarshal([]byte(as), &av) != nil || json.Unmarshal([]byte(bs), &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
