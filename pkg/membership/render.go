package membership

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

// TemplateData is what the executor template is executed with, once per tenant
// namespace.
type TemplateData struct {
	Namespace string
	Site      string
	HPCUser   string
	// Accounts are the tenant's accounts on the site, sorted.
	Accounts []string
	// Image is membership.executorImage of the OffloadConfig.
	Image string
	// ExecutorService and ExecutorPort are where the shadows read job logs from
	// the executor.
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

// ParseTemplate parses an executor template. Missing map keys are errors.
func ParseTemplate(text string) (*template.Template, error) {
	return template.New("executor").Option("missingkey=error").Funcs(templateFuncs).Parse(text)
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
