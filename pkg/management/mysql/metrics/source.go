/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

const (
	defaultQueriesResync = time.Minute
	// apiReadTimeout bounds each ConfigMap or Secret read.
	apiReadTimeout = 10 * time.Second
)

// errNoKey reports a referenced document missing its key. Like a missing
// object, it drops the document's queries instead of keeping the last good ones.
var errNoKey = errors.New("key not found")

// QuerySource feeds an Exporter the monitoring settings of the Cluster: it
// reads the custom query ConfigMaps and Secrets the Cluster references by name
// (the instance Role grants get on those names only) and re-reads them every
// minute, so edits apply without a restart.
type QuerySource struct {
	client    kubernetes.Interface
	namespace string
	exporter  *Exporter
	resync    time.Duration

	mu      sync.Mutex
	spec    *mysqlv1alpha1.MonitoringConfiguration
	changed chan struct{}
	// lastGood keeps each document's last parsed queries, so a failed read or
	// a bad edit does not drop metrics that were working.
	lastGood map[string][]CustomQuery
}

// NewQuerySource builds a QuerySource for the Cluster's namespace.
func NewQuerySource(c kubernetes.Interface, namespace string, e *Exporter) *QuerySource {
	return &QuerySource{
		client:    c,
		namespace: namespace,
		exporter:  e,
		resync:    defaultQueriesResync,
		changed:   make(chan struct{}, 1),
		lastGood:  map[string][]CustomQuery{},
	}
}

// Observe records the latest Cluster read by the role reconciler and wakes Run
// when its monitoring settings changed. It never blocks.
func (s *QuerySource) Observe(cluster *mysqlv1alpha1.Cluster) {
	spec := cluster.Spec.Monitoring
	if spec == nil {
		spec = &mysqlv1alpha1.MonitoringConfiguration{}
	}
	s.mu.Lock()
	same := s.spec != nil && equality.Semantic.DeepEqual(s.spec, spec)
	if !same {
		s.spec = spec.DeepCopy()
	}
	s.mu.Unlock()
	if !same {
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
}

// Run applies the monitoring settings whenever they change and re-reads the
// query documents every resync period, until ctx ends.
func (s *QuerySource) Run(ctx context.Context) {
	ticker := time.NewTicker(s.resync)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.changed:
		case <-ticker.C:
		}
		s.apply(ctx)
	}
}

func (s *QuerySource) apply(ctx context.Context) {
	s.mu.Lock()
	spec := s.spec
	s.mu.Unlock()
	if spec == nil {
		return
	}
	cfg := Config{CustomQueries: s.load(ctx, spec)}
	if spec.DisableDefaultQueries != nil {
		cfg.DisableDefaultQueries = *spec.DisableDefaultQueries
	}
	if spec.MetricsQueriesTTL != nil {
		cfg.TTL = spec.MetricsQueriesTTL.Duration
	}
	s.exporter.SetConfig(cfg)
}

// load reads and merges every referenced document, ConfigMaps first, then
// Secrets, each in list order. A query name seen again replaces the earlier
// one; a query whose metric names clash with another query's is dropped. A
// document that cannot be read or parsed keeps its last good queries, unless
// it is gone: a missing object or key drops them.
func (s *QuerySource) load(ctx context.Context, spec *mysqlv1alpha1.MonitoringConfiguration) []CustomQuery {
	log := logf.FromContext(ctx).WithName("custom-queries")
	type document struct {
		id   string
		read func() ([]byte, error)
	}
	var docs []document
	for _, ref := range spec.CustomQueriesConfigMap {
		docs = append(docs, document{
			id: "configmap/" + ref.Name + "/" + ref.Key,
			read: func() ([]byte, error) {
				ctx, cancel := context.WithTimeout(ctx, apiReadTimeout)
				defer cancel()
				cm, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					return nil, err
				}
				if v, ok := cm.Data[ref.Key]; ok {
					return []byte(v), nil
				}
				if v, ok := cm.BinaryData[ref.Key]; ok {
					return v, nil
				}
				return nil, fmt.Errorf("configmap %s has no key %q: %w", ref.Name, ref.Key, errNoKey)
			},
		})
	}
	for _, ref := range spec.CustomQueriesSecret {
		docs = append(docs, document{
			id: "secret/" + ref.Name + "/" + ref.Key,
			read: func() ([]byte, error) {
				ctx, cancel := context.WithTimeout(ctx, apiReadTimeout)
				defer cancel()
				sec, err := s.client.CoreV1().Secrets(s.namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					return nil, err
				}
				if v, ok := sec.Data[ref.Key]; ok {
					return v, nil
				}
				return nil, fmt.Errorf("secret %s has no key %q: %w", ref.Name, ref.Key, errNoKey)
			},
		})
	}

	byName := map[string]CustomQuery{}
	var order []string
	used := map[string]bool{}
	for _, d := range docs {
		used[d.id] = true
		queries, err := s.parse(d.read)
		switch {
		case apierrors.IsNotFound(err) || errors.Is(err, errNoKey):
			log.Info("Could not find custom queries", "source", d.id, "error", err.Error())
			delete(s.lastGood, d.id)
		case err != nil:
			log.Info("Could not load custom queries, keeping the last good ones",
				"source", d.id, "error", err.Error())
			queries = s.lastGood[d.id]
		default:
			s.lastGood[d.id] = queries
		}
		for _, q := range queries {
			if _, ok := byName[q.Name]; !ok {
				order = append(order, q.Name)
			}
			byName[q.Name] = q
		}
	}
	for id := range s.lastGood {
		if !used[id] {
			delete(s.lastGood, id)
		}
	}

	owner := map[string]string{} // metric name -> query name
	var out []CustomQuery
	for _, name := range order {
		q := byName[name]
		names := q.metricNames()
		if i := slices.IndexFunc(names, func(n string) bool { _, ok := owner[n]; return ok }); i >= 0 {
			log.Info("Dropped custom query publishing a metric another query already publishes",
				"query", q.Name, "metric", names[i], "otherQuery", owner[names[i]])
			continue
		}
		for _, n := range names {
			owner[n] = q.Name
		}
		out = append(out, q)
	}
	return out
}

func (s *QuerySource) parse(read func() ([]byte, error)) ([]CustomQuery, error) {
	data, err := read()
	if err != nil {
		return nil, err
	}
	return ParseCustomQueries(data)
}
