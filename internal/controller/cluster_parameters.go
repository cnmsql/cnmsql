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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// parametersStateAnnotation holds, as JSON, the operator's record of which
// spec.mysql.parameters values are applied to the running instances
// (Runtime) and which the pod template hash is rendered from (Template).
// See design/043-runtime-parameter-changes.md.
const parametersStateAnnotation = "cnmsql.cnmsql.co/parameters-state"

// parametersChangeStatus is the probe-derived classification of one changed
// parameter: runtime-settable (applied via SET GLOBAL) or restart-required
// (the server refused it, so the pod must be rolled to pick the value up from
// my.cnf).
type parametersChangeStatus string

const (
	paramDynamic         parametersChangeStatus = "dynamic"
	paramRestartRequired parametersChangeStatus = "restartRequired"
)

// parametersChange describes one user-parameter change to apply. The map key
// is the normalized name (lowercase, dashes folded to underscores) so spelling
// variants compare equal; Key carries the verbatim spelling the value is
// recorded under, and From/To the verbatim values.
type parametersChange struct {
	Key     string                 `json:"key"`
	From    string                 `json:"from,omitempty"`
	To      string                 `json:"to,omitempty"`
	Removed bool                   `json:"removed,omitempty"`
	Status  parametersChangeStatus `json:"status,omitempty"`
}

// normalizeParameterKey mirrors mysqlconfig's key normalization so a dash and
// an underscore spelling of the same variable compare equal.
func normalizeParameterKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
}

// diffParameters compares the desired spec.mysql.parameters against the values
// known applied to the running instances, returning changes keyed by normalized
// name. A parameter present only in known is a removal; a removal can never be
// applied with SET GLOBAL (there is no default to restore), so it is classified
// restart-required right away.
func diffParameters(desired, known map[string]string) map[string]*parametersChange {
	desiredNorm := normalizeParameterMap(desired)
	knownNorm := normalizeParameterMap(known)
	desiredKeys := verbatimParameterKeys(desired)
	knownKeys := verbatimParameterKeys(known)
	changes := map[string]*parametersChange{}
	for key, value := range desiredNorm {
		if knownNorm[key] == value {
			continue
		}
		changes[key] = &parametersChange{Key: desiredKeys[key], From: knownNorm[key], To: value}
	}
	for key, value := range knownNorm {
		if _, wanted := desiredNorm[key]; wanted {
			continue
		}
		changes[key] = &parametersChange{Key: knownKeys[key], From: value, Removed: true, Status: paramRestartRequired}
	}
	return changes
}

// verbatimParameterKeys maps each normalized parameter name to the verbatim
// spelling it is written under. When two spellings normalize to one name (the
// pathological duplicate of normalizeParameterMap), the same lexicographically
// last verbatim key wins.
func verbatimParameterKeys(params map[string]string) map[string]string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(params))
	for _, key := range keys {
		out[normalizeParameterKey(key)] = key
	}
	return out
}

// deleteNormalizedParameter removes every verbatim entry of m that normalizes
// to the given name.
func deleteNormalizedParameter(m map[string]string, normalized string) {
	for key := range m {
		if normalizeParameterKey(key) == normalized {
			delete(m, key)
		}
	}
}

// normalizeParameterMap folds a verbatim parameter map to normalized keys. When
// two spellings of the same variable are present (pathological, but the map
// shape allows it), the lexicographically last verbatim key wins, matching how
// mysqld resolves duplicate option lines.
func normalizeParameterMap(params map[string]string) map[string]string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(params))
	for _, key := range keys {
		out[normalizeParameterKey(key)] = params[key]
	}
	return out
}

// parametersState is the durable record of user-parameter application, kept on
// the Cluster under parametersStateAnnotation:
//   - Runtime holds the values known applied to running instances, either via
//     SET GLOBAL or baked into the pod template at creation/roll time. It is the
//     diff basis for spec.mysql.parameters.
//   - Template holds the values the pod template hash is rendered from. It only
//     moves for restart-required changes, so a dynamic-only change never rolls.
//   - Pending tracks the change set currently being probed, so probe evidence
//     accumulates across reconciles until every change is classified.
type parametersState struct {
	Runtime  map[string]string `json:"runtime,omitempty"`
	Template map[string]string `json:"template,omitempty"`
	// TemplateEpoch moves the pod template hash when a change cannot be
	// expressed through the Template map alone: removing a parameter that was
	// applied at runtime (and therefore never entered Template) leaves the
	// basis unchanged, yet only a restart undoes the value mysqld still holds.
	TemplateEpoch int                `json:"templateEpoch,omitempty"`
	Pending       *parametersPending `json:"pending,omitempty"`
}

// parametersPending is the in-flight diff: the reload token the pass was
// stamped with, the fingerprint of the change set it belongs to, and the
// per-parameter classification gathered so far.
type parametersPending struct {
	Token       string                       `json:"token"`
	Fingerprint string                       `json:"fingerprint"`
	Changes     map[string]*parametersChange `json:"changes"`
	// Answers records, per instance, that this change set was probed there by
	// any pass (the automatic one or a manual reload). A change settles only
	// when every instance answered: one ready instance applying a parameter
	// proves nothing about a lagging one, which may still refuse it.
	Answers map[string]bool `json:"answers,omitempty"`
}

// classifyReloadResults records the probe outcome of one reload pass into the
// change set. The instance manager reports every requested parameter either as
// applied (runtime-settable) or skipped (refused by the server, so restart-
// required). A restart-required classification is sticky: the conservative
// reading wins when a variable behaves differently across instances. Names
// outside the change set are ignored defensively.
func classifyReloadResults(changes map[string]*parametersChange, resp *webserver.ReloadResponse) bool {
	changed := false
	for _, name := range resp.Applied {
		change, ok := changes[normalizeParameterKey(name)]
		if !ok || change.Status == paramRestartRequired {
			continue
		}
		change.Status = paramDynamic
		changed = true
	}
	for name := range resp.Skipped {
		if change, ok := changes[normalizeParameterKey(name)]; ok && change.Status != paramRestartRequired {
			change.Status = paramRestartRequired
			changed = true
		}
	}
	return changed
}

// parameterChangesAnswered reports whether every named instance recorded an
// answer for the pending change set.
func (p *parametersPending) parameterChangesAnswered(instanceNames []string) bool {
	for _, name := range instanceNames {
		if !p.Answers[name] {
			return false
		}
	}
	return true
}

// parameterChangesClassified reports whether every change carries a probe
// verdict. Removals are classified at diff time, so only probed parameters
// gate this.
func parameterChangesClassified(changes map[string]*parametersChange) bool {
	for _, change := range changes {
		if change.Status == "" {
			return false
		}
	}
	return true
}

// absorbParameterChanges folds a fully classified change set into the state:
// every verdict reaches Runtime (the runtime now has, or is about to receive
// through the roll, the new value), while only restart-required changes reach
// Template so the pod template hash moves exactly when a roll is needed. It
// refuses (and does not mutate) while any change is still unclassified, and
// returns the dynamic and restart-required key lists for events.
func (s *parametersState) absorbParameterChanges(changes map[string]*parametersChange) (dynamic, restartRequired []string, ok bool) {
	if !parameterChangesClassified(changes) {
		return nil, nil, false
	}
	// Runtime and Template are verbatim-keyed complete maps: the pod template
	// hash is rendered from Template, so it must keep the exact keys and values
	// the existing hashes were computed from. Dynamic changes therefore leave
	// Template untouched — including not adding a newly introduced parameter,
	// whose absence is part of the basis the pods were hashed with.
	runtime := maps.Clone(s.Runtime)
	if runtime == nil {
		runtime = map[string]string{}
	}
	template := maps.Clone(s.Template)
	epochBump := false
	for key, change := range changes {
		if change.Removed {
			deleteNormalizedParameter(runtime, key)
			if templateHasKey(template, key) {
				// Dropping the key from the template basis changes the rendered
				// hash by itself.
				deleteNormalizedParameter(template, key)
			} else {
				// The basis never carried the key (it was applied at runtime),
				// so only the epoch can move the pod template hash and undo
				// the value mysqld still holds.
				epochBump = true
			}
			restartRequired = append(restartRequired, key)
			continue
		}
		runtime[change.Key] = change.To
		switch change.Status {
		case paramDynamic:
			dynamic = append(dynamic, key)
		case paramRestartRequired:
			if template == nil {
				template = map[string]string{}
			}
			deleteNormalizedParameter(template, key)
			template[change.Key] = change.To
			restartRequired = append(restartRequired, key)
		}
	}
	s.Runtime = runtime
	s.Template = template
	if epochBump {
		s.TemplateEpoch++
	}
	sort.Strings(dynamic)
	sort.Strings(restartRequired)
	return dynamic, restartRequired, true
}

// templateHasKey reports whether any verbatim key of m normalizes to the
// given name.
func templateHasKey(m map[string]string, normalized string) bool {
	for key := range m {
		if normalizeParameterKey(key) == normalized {
			return true
		}
	}
	return false
}

// fingerprintParameterChanges digests a change set so a later spec edit during
// an unfinished probe is recognized as a different, fresher change.
func fingerprintParameterChanges(changes map[string]*parametersChange) string {
	payload, err := json.Marshal(changes)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// reconcileParameters applies spec.mysql.parameters changes to the running
// instances without a rolling restart whenever the server can accept them
// (design/043). The diff basis is the Runtime record of the values already
// applied; changed parameters are probed through the shared reload pass, and
// only parameters the server refuses move the pod template hash (via the
// Template record) so the regular serialized roll picks them up from my.cnf.
//
// A change settles only when every instance answered the probe: one ready
// instance applying a parameter proves nothing about a lagging one, which may
// still refuse it and need the restart. Until then the diff keeps the change,
// so a spec edit re-probes the unabsorbed values everywhere.
//
// The pass never fails the reconcile over instance-level trouble: a missing
// Pod (mid-roll) is skipped — reconcileInstances recreates it — and a reload
// or patch error is logged and retried on a later pass. Only state writes
// propagate.
func (r *ClusterReconciler) reconcileParameters(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	state := loadParametersState(cluster)
	if state == nil {
		// First sight (or an unreadable record): the instance templates carry
		// the current spec values — the previous behavior hashed the pod
		// template straight from the spec — so seed both records with them,
		// verbatim so the hash basis stays byte-identical.
		state = &parametersState{
			Runtime:  maps.Clone(cluster.Spec.MySQL.Parameters),
			Template: maps.Clone(cluster.Spec.MySQL.Parameters),
		}
		return r.saveParametersState(ctx, cluster, state, "")
	}

	changes := diffParameters(cluster.Spec.MySQL.Parameters, state.Runtime)
	if len(changes) == 0 {
		// The change set is settled. The pending record lingers until the next
		// change replaces it, so a late-ready instance is caught up with the
		// changed parameters rather than the whole spec (which would overwrite
		// manual SET GLOBAL drift).
		return nil
	}

	dirty := false
	pendingStampToken := ""
	fingerprint := fingerprintParameterChanges(changes)
	if state.Pending == nil || state.Pending.Fingerprint != fingerprint {
		// A fresh (or edited mid-probe) change set: stamp a new reload token.
		// The annotation is written only here, so a newer manual reload token
		// is never reverted to an older automatic one.
		state.Pending = &parametersPending{
			Token:       metav1.Now().UTC().Format(time.RFC3339),
			Fingerprint: fingerprint,
			Changes:     changes,
			Answers:     map[string]bool{},
		}
		pendingStampToken = state.Pending.Token
		dirty = true
	}

	// Probe every ready instance that has not answered yet. Tokens are compared
	// for equality in the manual pass; this pass skips by answer, so a token a
	// manual reload stamped meanwhile cannot hide the change from this one.
	// Instance-level errors are logged inside the pass; only a non-NotFound Pod
	// read failure is returned, and the state saved below is written first so
	// partial evidence survives it.
	if err := r.applyReload(ctx, cluster, state.Pending.Token, probeParameters(state.Pending),
		func(name string, pod *corev1.Pod) bool {
			return state.Pending.Answers[name]
		},
		func(instance string, resp *webserver.ReloadResponse) {
			if state.Pending == nil {
				return
			}
			if classifyReloadResults(state.Pending.Changes, resp) {
				dirty = true
			}
			state.Pending.Answers[instance] = true
			dirty = true
		}); err != nil {
		logf.FromContext(ctx).WithName("reload").Error(err, "Could not read an instance Pod for the parameter probe, will retry")
	}

	// Settle and absorb once every instance answered and every parameter is
	// classified. Removals are classified at diff time; the verdicts come from
	// the instances.
	if parameterChangesClassified(state.Pending.Changes) &&
		state.Pending.parameterChangesAnswered(cluster.Status.InstanceNames) {
		if dynamic, restartRequired, ok := state.absorbParameterChanges(state.Pending.Changes); ok {
			state.emitParameterEvents(r, cluster, dynamic, restartRequired)
			dirty = true
		}
	}
	if !dirty {
		return nil
	}
	// Stamp the reload token only here, and only when this pass created the
	// pending record; the save stamps it after its diff base is taken, so the
	// annotation actually reaches the API.
	return r.saveParametersState(ctx, cluster, state, pendingStampToken)
}

// emitParameterEvents surfaces the probe verdicts. A refused parameter is
// always visible: my.cnf holds the new value and the roll will apply it, but
// the operator should know the runtime could not.
func (s *parametersState) emitParameterEvents(r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster, dynamic, restartRequired []string) {
	if r.Recorder == nil {
		return
	}
	if len(dynamic) > 0 {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "ParametersAppliedAtRuntime",
			"Applied %d parameter(s) at runtime: %s", len(dynamic), strings.Join(dynamic, ", "))
	}
	if len(restartRequired) > 0 {
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "ParametersRequireRestart",
			"Parameter(s) %s were refused at runtime and require a restart", strings.Join(restartRequired, ", "))
	}
}

// applyReload runs one reload pass over the cluster's instances: it re-applies
// params to every ready instance the caller has not excluded via skip, stamps
// the token on each probed Pod, and hands every response to record so the
// caller can accumulate evidence. This is the single reload pass shared by the
// automatic parameter path and the manual `kubectl cnmsql reload` annotation.
//
// Instance-level trouble never aborts the pass or fails the reconcile: a
// missing Pod (mid-roll) is skipped because reconcileInstances recreates it,
// and reload or patch errors are logged and retried on a later pass. Only a
// failed Pod read other than NotFound is returned, so an API-server hiccup
// still requeues.
func (r *ClusterReconciler) applyReload(ctx context.Context, cluster *mysqlv1alpha1.Cluster, token string, params map[string]string,
	skip func(instance string, pod *corev1.Pod) bool, record func(instance string, resp *webserver.ReloadResponse)) error {
	log := logf.FromContext(ctx).WithName("reload")
	control := r.instanceControlClient()

	for _, name := range cluster.Status.InstanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if skip != nil && skip(name, pod) {
			continue
		}
		if !podReady(pod) {
			// Skip not-ready instances; they pick up the desired config from the
			// rendered my.cnf on (re)start, and a later pass reloads them once ready.
			continue
		}

		resp, err := control.Reload(ctx, cluster, name, webserver.ReloadRequest{Parameters: params})
		if err != nil {
			// One unreachable instance must not block the others; a later pass
			// retries it.
			log.Error(err, "Could not probe instance for a parameter reload", "instance", name)
			continue
		}
		if len(resp.Skipped) > 0 {
			log.Info("Some parameters could not be applied at runtime (restart required)",
				"instance", name, "skipped", resp.Skipped)
		}
		if record != nil {
			record(name, resp)
		}
		log.Info("Applied configuration reload", "instance", name, "applied", resp.Applied)

		before := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[reloadAppliedAnnotation] = token
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			log.Error(err, "Could not record the reload token on the Pod", "instance", name)
		}
	}
	return nil
}

// probeParameters builds the SET GLOBAL request for the pending change set:
// only changed parameters are sent, so manual SET GLOBAL drift on unchanged
// parameters survives until an explicit reload. Removed parameters are
// excluded: a global cannot be unset without knowing its default, so removals
// converge through the roll instead.
func probeParameters(pending *parametersPending) map[string]string {
	params := make(map[string]string, len(pending.Changes))
	for key, change := range pending.Changes {
		if change.Removed {
			continue
		}
		params[key] = change.To
	}
	return params
}

// loadParametersState parses the state annotation. An absent or unreadable
// record returns nil; the caller then seeds from the spec.
func loadParametersState(cluster *mysqlv1alpha1.Cluster) *parametersState {
	raw, ok := cluster.Annotations[parametersStateAnnotation]
	if !ok || raw == "" {
		return nil
	}
	state := &parametersState{}
	if err := json.Unmarshal([]byte(raw), state); err != nil {
		return nil
	}
	// A record written before per-instance answers existed (or a partially
	// populated one) must not carry a nil map into the pass.
	if state.Pending != nil && state.Pending.Answers == nil {
		state.Pending.Answers = map[string]bool{}
	}
	return state
}

// saveParametersState persists the parameter record. A non-empty reloadToken
// stamps reloadAnnotation — passed only when this pass created the pending
// record, so a newer manual reload token is never reverted to an older
// automatic one. The token is stamped after the diff base is copied, or the
// patch would not carry it.
func (r *ClusterReconciler) saveParametersState(ctx context.Context, cluster *mysqlv1alpha1.Cluster, state *parametersState, reloadToken string) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[parametersStateAnnotation] = string(payload)
	if reloadToken != "" {
		cluster.Annotations[reloadAnnotation] = reloadToken
	}
	return r.Patch(ctx, cluster, client.MergeFrom(before))
}
