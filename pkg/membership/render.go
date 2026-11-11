package membership

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// TemplateData is what the executor template is executed with, once per tenant
// namespace, and the offboard template once per namespace that left the site.
type TemplateData struct {
	Namespace string
	Site      string
	HPCUser   string
	// Accounts are the tenant's accounts on the site, sorted; in the offboard
	// template, those the namespace had before it left.
	Accounts []string
	// Queues are the names of the site's membership.queues, in their order,
	// less those none of whose flavors has quota.
	Queues []string
	// QueuesOf maps each of Accounts to the names of Queues it has the
	// LocalQueue <site>-<account>-<queue> of, in their order: a queue whose
	// name is too long or held by another account, or none of whose flavors
	// has quota in the account's quota, is left out for that account. Each
	// account has the LocalQueue <site>-<account> of every flavor too.
	QueuesOf map[string][]string
	// Image is membership.executorImage of the OffloadConfig.
	Image string
	// ExecutorService and ExecutorPort are where the virtual node reads job
	// logs from the executor.
	ExecutorService string
	ExecutorPort    int32
}

var templateFuncs = template.FuncMap{
	"join":  func(sep string, l []string) string { return strings.Join(l, sep) },
	"quote": strconv.Quote,
	"toJson": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
}

// ParseTemplate parses key of a template ConfigMap's data. Every other key
// whose name ends in .tpl.yaml is parsed as a template of that name, for
// {{ template "<key>" . }}; the function file returns any key's content as it
// is. Missing map keys are errors.
func ParseTemplate(data map[string]string, key string) (*template.Template, error) {
	text, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("no key %q", key)
	}
	file := func(k string) (string, error) {
		v, ok := data[k]
		if !ok {
			return "", fmt.Errorf("no key %q", k)
		}
		return v, nil
	}
	t, err := template.New(key).Option("missingkey=error").Funcs(templateFuncs).
		Funcs(template.FuncMap{"file": file}).Parse(text)
	if err != nil {
		return nil, err
	}
	var parts []string
	for k := range data {
		if k != key && strings.HasSuffix(k, ".tpl.yaml") {
			parts = append(parts, k)
		}
	}
	slices.Sort(parts)
	for _, k := range parts {
		if _, err := t.New(k).Parse(data[k]); err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
	}
	return t, nil
}

// renderObjects executes the template and splits its output into objects.
// Empty documents are dropped.
func renderObjects(t *template.Template, data TemplateData) ([]*unstructured.Unstructured, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return nil, err
	}
	r := utilyaml.NewYAMLReader(bufio.NewReader(&buf))
	var objs []*unstructured.Unstructured
	for i := 0; ; i++ {
		doc, err := r.Read()
		if errors.Is(err, io.EOF) {
			return objs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		m := map[string]any{}
		if err := yaml.Unmarshal(doc, &m); err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		if len(m) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: m}
		if u.GetAPIVersion() == "" || u.GetKind() == "" || u.GetName() == "" {
			return nil, fmt.Errorf("document %d: apiVersion, kind and metadata.name are required", i)
		}
		objs = append(objs, u)
	}
}
