package cfn2tf

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Expr is an HCL expression: string, int64, float64, bool, []Expr, *Object, Raw, Template or Call.
type Expr any

// Raw is an expression written as is, such as aws_s3_bucket.logs.arn.
type Raw string

// Template is a string with interpolations; parts are string literals or expressions.
type Template []Expr

// Call is a function call.
type Call struct {
	Name string
	Args []Expr
}

// Object is an ordered object (map) expression.
type Object struct {
	Keys   []string
	Values map[string]Expr
}

// NewObject makes an empty object.
func NewObject() *Object { return &Object{Values: map[string]Expr{}} }

// Set adds or replaces a key.
func (o *Object) Set(k string, v Expr) {
	if _, ok := o.Values[k]; !ok {
		o.Keys = append(o.Keys, k)
	}
	o.Values[k] = v
}

// Sorted returns the object with keys in order (for maps whose order carries no meaning).
func (o *Object) Sorted() *Object {
	sort.Strings(o.Keys)
	return o
}

// Block is an HCL block.
type Block struct {
	Type   string
	Labels []string
	Attrs  []Attr
	Blocks []*Block
}

// Attr is one attribute of a block.
type Attr struct {
	Name  string
	Value Expr
}

// Set adds an attribute; nil values are skipped.
func (b *Block) Set(name string, v Expr) *Block {
	if v != nil {
		b.Attrs = append(b.Attrs, Attr{name, v})
	}
	return b
}

// Add adds a nested block and returns it.
func (b *Block) Add(typ string, labels ...string) *Block {
	c := &Block{Type: typ, Labels: labels}
	b.Blocks = append(b.Blocks, c)
	return c
}

var ident = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// Render writes blocks as HCL.
func Render(blocks []*Block) string {
	var b strings.Builder
	for i, bl := range blocks {
		if i > 0 {
			b.WriteString("\n")
		}
		renderBlock(&b, bl, 0)
	}
	return b.String()
}

func renderBlock(b *strings.Builder, bl *Block, depth int) {
	ind := strings.Repeat("  ", depth)
	b.WriteString(ind + bl.Type)
	for _, l := range bl.Labels {
		b.WriteString(" " + quote(l))
	}
	b.WriteString(" {\n")
	// Align "=" across runs of single-line attributes, as terraform fmt does.
	widths := alignWidths(len(bl.Attrs), func(i int) (string, bool) { return bl.Attrs[i].Name, multiline(bl.Attrs[i].Value) })
	for i, a := range bl.Attrs {
		b.WriteString(fmt.Sprintf("%s  %s%s = %s\n", ind, a.Name, strings.Repeat(" ", widths[i]-len(a.Name)), expr(a.Value, depth+1)))
	}
	for i, c := range bl.Blocks {
		if i > 0 || len(bl.Attrs) > 0 {
			b.WriteString("\n")
		}
		renderBlock(b, c, depth+1)
	}
	b.WriteString(ind + "}\n")
}

// alignWidths gives each entry the width to pad its name to: the longest name in its run of
// consecutive single-line entries; a multi-line entry ends a run and is not padded.
func alignWidths(n int, at func(int) (string, bool)) []int {
	w := make([]int, n)
	start := 0
	flush := func(end int) {
		m := 0
		for j := start; j < end; j++ {
			name, _ := at(j)
			m = max(m, len(name))
		}
		for j := start; j < end; j++ {
			w[j] = m
		}
	}
	for i := 0; i < n; i++ {
		name, multi := at(i)
		if multi {
			flush(i)
			w[i] = len(name)
			start = i + 1
		}
	}
	flush(n)
	return w
}

func multiline(v Expr) bool {
	switch x := v.(type) {
	case *Object:
		return len(x.Keys) > 0
	case Call:
		for _, a := range x.Args {
			if multiline(a) {
				return true
			}
		}
	case []Expr:
		for _, a := range x {
			if multiline(a) {
				return true
			}
		}
	}
	return false
}

// expr renders an expression at a nesting depth (for indentation of objects).
func expr(v Expr, depth int) string {
	ind := strings.Repeat("  ", depth)
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return quote(x)
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case Raw:
		return string(x)
	case Template:
		if len(x) == 1 {
			if r, ok := x[0].(Raw); ok {
				return string(r)
			}
		}
		var b strings.Builder
		b.WriteString(`"`)
		for _, p := range x {
			if s, ok := p.(string); ok {
				b.WriteString(escape(s))
			} else {
				b.WriteString("${" + expr(p, depth) + "}")
			}
		}
		b.WriteString(`"`)
		return b.String()
	case Call:
		args := make([]string, len(x.Args))
		for i, a := range x.Args {
			args[i] = expr(a, depth)
		}
		return x.Name + "(" + strings.Join(args, ", ") + ")"
	case []Expr:
		if !multiline(x) {
			parts := make([]string, len(x))
			for i, a := range x {
				parts[i] = expr(a, depth)
			}
			return "[" + strings.Join(parts, ", ") + "]"
		}
		var b strings.Builder
		b.WriteString("[\n")
		for _, a := range x {
			b.WriteString(ind + "  " + expr(a, depth+1) + ",\n")
		}
		b.WriteString(ind + "]")
		return b.String()
	case *Object:
		if len(x.Keys) == 0 {
			return "{}"
		}
		widths := alignWidths(len(x.Keys), func(i int) (string, bool) { return key(x.Keys[i]), multiline(x.Values[x.Keys[i]]) })
		var b strings.Builder
		b.WriteString("{\n")
		for i, k := range x.Keys {
			kk := key(k)
			b.WriteString(fmt.Sprintf("%s  %s%s = %s\n", ind, kk, strings.Repeat(" ", widths[i]-len(kk)), expr(x.Values[k], depth+1)))
		}
		b.WriteString(ind + "}")
		return b.String()
	}
	panic(fmt.Sprintf("cfn2tf: cannot render %T", v))
}

func key(k string) string {
	if ident.MatchString(k) {
		return k
	}
	return quote(k)
}

func quote(s string) string { return `"` + escape(s) + `"` }

// escape makes a string literal safe in HCL, including template sequences.
func escape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "\t", `\t`, "${", "$${", "%{", "%%{")
	return r.Replace(s)
}
