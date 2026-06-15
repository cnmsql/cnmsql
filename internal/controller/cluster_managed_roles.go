/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

// reconcileManagedRoles drives spec.managed.roles against the primary instance.
// It lists the live MySQL users, diffs them against the desired roles, and
// issues the minimal CREATE/ALTER/DROP needed. Password material is read from
// Kubernetes Secrets by the operator (or generated when none is referenced) and
// re-applied only when the source Secret changes. The MySQL state is read once
// and the per-role outcomes are recorded in status.managedRolesStatus.
func (r *ClusterReconciler) reconcileManagedRoles(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	if cluster.Spec.Managed == nil || len(cluster.Spec.Managed.Roles) == 0 {
		return nil
	}
	primary := cluster.Status.CurrentPrimary
	if primary == "" {
		return nil
	}
	log := logf.FromContext(ctx)
	controlClient := r.instanceControlClient()

	listed, err := controlClient.ListUsers(ctx, cluster, primary)
	if err != nil {
		return fmt.Errorf("listing managed roles on %s: %w", primary, err)
	}
	observed := make(map[string]user.UserInfo, len(listed.Users))
	for _, u := range listed.Users {
		observed[roleKey(u.Name, u.Host)] = u
	}

	status := &mysqlv1alpha1.ManagedRolesStatus{
		ByStatus:        map[mysqlv1alpha1.ManagedRoleStatus][]string{},
		CannotReconcile: map[string][]string{},
		PasswordStatus:  map[string]mysqlv1alpha1.RolePasswordState{},
	}
	// Carry forward the previously applied password versions so we only re-apply
	// passwords whose source Secret changed.
	prevPasswords := map[string]mysqlv1alpha1.RolePasswordState{}
	if cluster.Status.ManagedRolesStatus != nil {
		prevPasswords = cluster.Status.ManagedRolesStatus.PasswordStatus
	}

	for i := range cluster.Spec.Managed.Roles {
		role := &cluster.Spec.Managed.Roles[i]
		state, err := r.reconcileManagedRole(ctx, cluster, primary, role, observed, prevPasswords[role.Name])
		if err != nil {
			log.Info("Managed role reconciliation failed", "role", role.Name, "error", err.Error())
			if r.Recorder != nil {
				r.Recorder.Event(cluster, corev1.EventTypeWarning, "ManagedRoleError",
					fmt.Sprintf("role %s: %v", role.Name, err))
			}
			status.ByStatus[mysqlv1alpha1.ManagedRolePendingReconciliation] =
				append(status.ByStatus[mysqlv1alpha1.ManagedRolePendingReconciliation], role.Name)
			status.CannotReconcile[role.Name] = append(status.CannotReconcile[role.Name], err.Error())
			continue
		}
		status.ByStatus[mysqlv1alpha1.ManagedRoleReconciled] =
			append(status.ByStatus[mysqlv1alpha1.ManagedRoleReconciled], role.Name)
		if state != nil {
			status.PasswordStatus[role.Name] = *state
		}
	}

	sortRoleStatus(status)
	return r.patchManagedRolesStatus(ctx, cluster, status)
}

// reconcileManagedRole reconciles a single role and returns the password state
// to record (nil when the role was dropped or carries no password).
func (r *ClusterReconciler) reconcileManagedRole(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	primary string,
	role *mysqlv1alpha1.RoleConfiguration,
	observed map[string]user.UserInfo,
	prevPassword mysqlv1alpha1.RolePasswordState,
) (*mysqlv1alpha1.RolePasswordState, error) {
	host := defaultHost(role.Host)
	key := roleKey(role.Name, host)
	current, exists := observed[key]

	if role.Ensure == mysqlv1alpha1.EnsureAbsent {
		if !exists {
			return nil, nil
		}
		if err := r.ControlClient.DropUser(ctx, cluster, primary, user.DropUserRequest{
			Name: role.Name, Host: host,
		}); err != nil {
			return nil, err
		}
		r.recordRoleEvent(cluster, "ManagedRoleDropped", role.Name)
		return nil, nil
	}

	password, secretRV, err := r.resolveRolePassword(ctx, cluster, role)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := r.ControlClient.CreateUser(ctx, cluster, primary, user.CreateUserRequest{
			Name:                  role.Name,
			Host:                  host,
			Password:              password,
			Superuser:             role.Superuser,
			MaxUserConnections:    role.MaxUserConnections,
			MaxQueriesPerHour:     role.MaxQueriesPerHour,
			MaxUpdatesPerHour:     role.MaxUpdatesPerHour,
			MaxConnectionsPerHour: role.MaxConnectionsPerHour,
			RequireTLS:            normalizeRequireTLS(role.RequireTLS),
			Privileges:            toPrivileges(role.Privileges),
		}); err != nil {
			return nil, err
		}
		r.recordRoleEvent(cluster, "ManagedRoleCreated", role.Name)
		return &mysqlv1alpha1.RolePasswordState{
			SecretResourceVersion: secretRV,
			LastApplied:           metav1.Now(),
		}, nil
	}

	// The user exists: build a minimal ALTER from the precise diff.
	alter, changed := diffRole(role, host, current)
	passwordChanged := secretRV != prevPassword.SecretResourceVersion
	passwordState := prevPassword
	if passwordChanged {
		alter.Password = &password
		passwordState = mysqlv1alpha1.RolePasswordState{
			SecretResourceVersion: secretRV,
			LastApplied:           metav1.Now(),
		}
		changed = true
	}
	if changed {
		if err := r.ControlClient.AlterUser(ctx, cluster, primary, alter); err != nil {
			return nil, err
		}
		r.recordRoleEvent(cluster, "ManagedRoleUpdated", role.Name)
	}
	return &passwordState, nil
}

// diffRole computes the ALTER USER request for the attributes that differ
// between the desired role and the observed user. The returned bool reports
// whether anything (besides password, handled by the caller) changed.
func diffRole(role *mysqlv1alpha1.RoleConfiguration, host string, current user.UserInfo) (user.AlterUserRequest, bool) {
	alter := user.AlterUserRequest{Name: role.Name, Host: host}
	changed := false

	if role.MaxUserConnections != current.MaxUserConnections {
		v := role.MaxUserConnections
		alter.MaxUserConnections = &v
		changed = true
	}
	if role.MaxQueriesPerHour != current.MaxQueriesPerHour {
		v := role.MaxQueriesPerHour
		alter.MaxQueriesPerHour = &v
		changed = true
	}
	if role.MaxUpdatesPerHour != current.MaxUpdatesPerHour {
		v := role.MaxUpdatesPerHour
		alter.MaxUpdatesPerHour = &v
		changed = true
	}
	if role.MaxConnectionsPerHour != current.MaxConnectionsPerHour {
		v := role.MaxConnectionsPerHour
		alter.MaxConnectionsPerHour = &v
		changed = true
	}
	if normalizeRequireTLS(role.RequireTLS) != normalizeRequireTLS(current.RequireTLS) {
		v := normalizeRequireTLS(role.RequireTLS)
		alter.RequireTLS = &v
		changed = true
	}
	if !grantsSatisfied(current.Grants, role) {
		su := role.Superuser
		alter.Superuser = &su
		privs := toPrivileges(role.Privileges)
		alter.Privileges = &privs
		changed = true
	}
	return alter, changed
}

// grantsSatisfied reports whether every desired grant is already present in the
// observed SHOW GRANTS output. It is a conservative check: when a desired grant
// is not found verbatim (modulo case and identifier quoting), the role is
// re-granted.
func grantsSatisfied(observed []string, role *mysqlv1alpha1.RoleConfiguration) bool {
	have := map[string]bool{}
	for _, g := range observed {
		for _, tok := range parseGrantTokens(g) {
			have[tok] = true
		}
	}
	if role.Superuser {
		return have["all privileges@*.*"]
	}
	for _, p := range role.Privileges {
		on := p.On
		if on == "" {
			on = "*.*"
		}
		for _, priv := range p.Privileges {
			tok := strings.ToLower(strings.TrimSpace(priv)) + "@" + normalizeGrantTarget(on)
			if !have[tok] {
				return false
			}
		}
	}
	return true
}

// parseGrantTokens turns a SHOW GRANTS line into "priv@target" tokens.
func parseGrantTokens(grant string) []string {
	upper := grant
	idx := strings.Index(strings.ToUpper(upper), " ON ")
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(upper)), "GRANT ") || idx < 0 {
		return nil
	}
	privPart := strings.TrimSpace(upper[len("GRANT "):idx])
	rest := strings.TrimSpace(upper[idx+len(" ON "):])
	toIdx := strings.Index(strings.ToUpper(rest), " TO ")
	if toIdx < 0 {
		return nil
	}
	target := normalizeGrantTarget(strings.TrimSpace(rest[:toIdx]))
	var tokens []string
	for p := range strings.SplitSeq(privPart, ",") {
		tokens = append(tokens, strings.ToLower(strings.TrimSpace(p))+"@"+target)
	}
	return tokens
}

// normalizeGrantTarget strips identifier backticks and lowercases a grant
// target so "`db`.*" and "db.*" compare equal.
func normalizeGrantTarget(target string) string {
	return strings.ToLower(strings.ReplaceAll(target, "`", ""))
}

// resolveRolePassword returns the password and the source Secret's
// resourceVersion. When the role references no Secret, the operator generates a
// password and persists it in a Secret named "<cluster>-<role>".
func (r *ClusterReconciler) resolveRolePassword(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	role *mysqlv1alpha1.RoleConfiguration,
) (string, string, error) {
	if role.PasswordSecret != nil {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: cluster.Namespace, Name: role.PasswordSecret.Name,
		}, secret); err != nil {
			return "", "", fmt.Errorf("reading password secret %s: %w", role.PasswordSecret.Name, err)
		}
		data, ok := secret.Data[role.PasswordSecret.Key]
		if !ok {
			return "", "", fmt.Errorf("password secret %s missing key %s",
				role.PasswordSecret.Name, role.PasswordSecret.Key)
		}
		return string(data), secret.ResourceVersion, nil
	}
	return r.ensureGeneratedRolePassword(ctx, cluster, role.Name)
}

// ensureGeneratedRolePassword reads (or creates) the operator-managed password
// Secret for a role and returns the password and its resourceVersion.
func (r *ClusterReconciler) ensureGeneratedRolePassword(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	roleName string,
) (string, string, error) {
	name := cluster.Name + "-" + roleName
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, secret)
	if err == nil {
		if data, ok := secret.Data["password"]; ok {
			return string(data), secret.ResourceVersion, nil
		}
	} else if client.IgnoreNotFound(err) != nil {
		return "", "", err
	}

	password, err := randomPassword()
	if err != nil {
		return "", "", err
	}
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labelsFor(cluster, "", ""),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"password": password},
	}
	if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return "", "", err
	}
	if err := r.Create(ctx, secret); err != nil {
		return "", "", err
	}
	// Re-read to obtain the assigned resourceVersion.
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, secret); err != nil {
		return "", "", err
	}
	return password, secret.ResourceVersion, nil
}

func (r *ClusterReconciler) recordRoleEvent(cluster *mysqlv1alpha1.Cluster, reason, roleName string) {
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, reason, "managed role "+roleName)
	}
}

// patchManagedRolesStatus writes status.managedRolesStatus without disturbing
// the rest of the status subresource.
func (r *ClusterReconciler) patchManagedRolesStatus(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	status *mysqlv1alpha1.ManagedRolesStatus,
) error {
	latest := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	latest.Status.ManagedRolesStatus = status
	cluster.Status.ManagedRolesStatus = status
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

func roleKey(name, host string) string {
	return name + "@" + defaultHost(host)
}

func defaultHost(host string) string {
	if host == "" {
		return "%"
	}
	return host
}

// RequireTLS values shared between the role spec and the SQL builder.
const (
	requireTLSNone = "none"
	requireTLSSSL  = "ssl"
	requireTLSX509 = "x509"
)

func normalizeRequireTLS(v string) string {
	switch strings.ToLower(v) {
	case requireTLSSSL:
		return requireTLSSSL
	case requireTLSX509:
		return requireTLSX509
	default:
		return requireTLSNone
	}
}

func toPrivileges(in []mysqlv1alpha1.RolePrivilege) []user.Privilege {
	if len(in) == 0 {
		return nil
	}
	out := make([]user.Privilege, 0, len(in))
	for _, p := range in {
		out = append(out, user.Privilege{Privileges: p.Privileges, On: p.On})
	}
	return out
}

func sortRoleStatus(status *mysqlv1alpha1.ManagedRolesStatus) {
	for k := range status.ByStatus {
		sort.Strings(status.ByStatus[k])
	}
}
