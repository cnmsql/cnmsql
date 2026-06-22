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

package cmd

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

// newDatabaseUserCommand builds the declarative `databaseuser` noun, which edits
// DatabaseUser custom resources (and their password Secrets) and lets the
// operator reconcile them. It is the day-2 front-end to the DatabaseUser CRD.
func newDatabaseUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "databaseuser",
		Aliases: []string{"dbuser"},
		Short:   "Manage installation-wide DatabaseUser resources",
		Long: "Create and manage DatabaseUser custom resources: standalone, " +
			"installation-wide MySQL accounts reconciled by the operator.\n\n" +
			"Unlike `user`, these commands do not talk to MySQL directly; they edit " +
			"the DatabaseUser object and its password Secret, and the operator applies " +
			"the change. This preserves status, password rotation, and conflict/adoption " +
			"semantics.",
		Example: `  # List DatabaseUsers and their applied state
  kubectl cnmsql databaseuser list

  # Create a user with a generated password and a grant
  kubectl cnmsql databaseuser create tenant --cluster cluster-sample --generate \
    --grant "SELECT,INSERT ON app.*"

  # Rotate the password
  kubectl cnmsql databaseuser passwd tenant --generate

  # Adopt a pre-existing MySQL account on UserConflict
  kubectl cnmsql databaseuser adopt tenant

  # Scaffold a constrained DBaaS admin (ALL, no cluster-control privileges)
  kubectl cnmsql databaseuser dbaas tenant-admin --cluster cluster-sample`,
	}
	cmd.AddCommand(
		newDBUserListCommand(),
		newDBUserCreateCommand(),
		newDBUserGrantCommand(),
		newDBUserPasswdCommand(),
		newDBUserAdoptCommand(),
		newDBUserDropCommand(),
		newDBUserDBaaSCommand(),
	)
	return cmd
}

func newDBUserListCommand() *cobra.Command {
	var clusterFilter string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List DatabaseUser resources and their state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			list := &mysqlv1alpha1.DatabaseUserList{}
			if err := env.Client.List(ctx, list, client.InNamespace(env.Namespace)); err != nil {
				return err
			}
			rows := make([][]string, 0, len(list.Items))
			for i := range list.Items {
				du := &list.Items[i]
				if clusterFilter != "" && du.Spec.Cluster.Name != clusterFilter {
					continue
				}
				rows = append(rows, []string{
					du.Name, du.UserName(), du.Spec.Cluster.Name,
					appliedString(du.Status.Applied), readyReason(du.Status.Conditions),
				})
			}
			plugin.Table([]string{"NAME", "USER", "CLUSTER", "APPLIED", "REASON"}, rows)
			return nil
		},
	}
	cmd.Flags().StringVar(&clusterFilter, "cluster", "", "only show users for this cluster")
	return cmd
}

func newDBUserCreateCommand() *cobra.Command {
	var (
		clusterName string
		userName    string
		host        string
		secretRef   string
		generate    bool
		grants      []string
		superuser   bool
		requireTLS  string
		reclaim     string
	)
	cmd := &cobra.Command{
		Use:   "create NAME --cluster CLUSTER",
		Short: "Create a DatabaseUser resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if clusterName == "" {
				return fmt.Errorf("--cluster is required")
			}
			env, err := newEnv()
			if err != nil {
				return err
			}
			name := args[0]
			parsedGrants, err := parseGrantFlags(grants)
			if err != nil {
				return err
			}
			du := &mysqlv1alpha1.DatabaseUser{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
				Spec: mysqlv1alpha1.DatabaseUserSpec{
					Cluster:    mysqlv1alpha1.LocalObjectReference{Name: clusterName},
					Name:       userName,
					Host:       host,
					Superuser:  superuser,
					RequireTLS: requireTLS,
					Grants:     parsedGrants,
				},
			}
			if reclaim != "" {
				du.Spec.ReclaimPolicy = reclaim
			}
			secretName, secretKey := secretRefOrDefault(secretRef, name)
			du.Spec.PasswordSecret = &mysqlv1alpha1.SecretKeySelector{Name: secretName, Key: secretKey}
			if generate {
				pw, err := writeGeneratedPassword(ctx, env, secretName, secretKey)
				if err != nil {
					return err
				}
				fmt.Printf("generated password (store it safely): %s\n", pw)
			}
			if err := env.Client.Create(ctx, du); err != nil {
				return fmt.Errorf("creating databaseuser: %w", err)
			}
			fmt.Printf("created databaseuser %q for cluster %q\n", name, clusterName)
			return nil
		},
	}
	cmd.Flags().StringVar(&clusterName, "cluster", "", "target cluster (required)")
	cmd.Flags().StringVar(&userName, "user", "", "MySQL user name (default: resource name)")
	cmd.Flags().StringVar(&host, "host", "%", "host pattern")
	cmd.Flags().StringVar(&secretRef, "password-secret", "", "password Secret as name/key (default: <name>-pw/password)")
	cmd.Flags().BoolVar(&generate, "generate", false, "generate a password and write it to the Secret")
	cmd.Flags().StringArrayVar(&grants, "grant", nil, `grant as "PRIV[,PRIV] ON target" (repeatable)`)
	cmd.Flags().BoolVar(&superuser, "superuser", false, "grant superuser (unsafe for multi-tenant)")
	cmd.Flags().StringVar(&requireTLS, "require-tls", "", "TLS requirement: none|ssl|x509")
	cmd.Flags().StringVar(&reclaim, "reclaim", "", "reclaim policy on delete: retain|delete")
	return cmd
}

func newDBUserGrantCommand() *cobra.Command {
	var (
		add    []string
		remove []string
	)
	cmd := &cobra.Command{
		Use:   "grant NAME",
		Short: "Add or remove grants on a DatabaseUser",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			return patchDatabaseUser(ctx, env, args[0], func(du *mysqlv1alpha1.DatabaseUser) error {
				toAdd, err := parseGrantFlags(add)
				if err != nil {
					return err
				}
				toRemove, err := parseGrantFlags(remove)
				if err != nil {
					return err
				}
				du.Spec.Grants = applyGrantDelta(du.Spec.Grants, toAdd, toRemove)
				return nil
			})
		},
	}
	cmd.Flags().StringArrayVar(&add, "add", nil, `grant to add: "PRIV[,PRIV] ON target" (repeatable)`)
	cmd.Flags().StringArrayVar(&remove, "remove", nil, `grant to remove: "PRIV[,PRIV] ON target" (repeatable)`)
	return cmd
}

func newDBUserPasswdCommand() *cobra.Command {
	var (
		generate    bool
		passwdStdin bool
	)
	cmd := &cobra.Command{
		Use:   "passwd NAME",
		Short: "Rotate a DatabaseUser's password",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			du := &mysqlv1alpha1.DatabaseUser{}
			if err := env.Client.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: args[0]}, du); err != nil {
				return err
			}
			if du.Spec.PasswordSecret == nil {
				return fmt.Errorf("databaseuser %q has no passwordSecret to rotate", args[0])
			}
			name, key := du.Spec.PasswordSecret.Name, du.Spec.PasswordSecret.Key
			switch {
			case generate:
				pw, err := writeGeneratedPassword(ctx, env, name, key)
				if err != nil {
					return err
				}
				fmt.Printf("rotated password (store it safely): %s\n", pw)
			case passwdStdin:
				pw, err := plugin.ReadPassword(true)
				if err != nil {
					return err
				}
				if err := writePassword(ctx, env, name, key, pw); err != nil {
					return err
				}
				fmt.Printf("rotated password for %q\n", args[0])
			default:
				return fmt.Errorf("specify --generate or --password-stdin")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&generate, "generate", false, "generate a new password")
	cmd.Flags().BoolVar(&passwdStdin, "password-stdin", false, "read the new password from stdin")
	return cmd
}

func newDBUserAdoptCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "adopt NAME",
		Short: "Adopt a pre-existing MySQL account (resolves UserConflict)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			if err := patchDatabaseUser(ctx, env, args[0], func(du *mysqlv1alpha1.DatabaseUser) error {
				if du.Annotations == nil {
					du.Annotations = map[string]string{}
				}
				du.Annotations[mysqlv1alpha1.DatabaseUserAdoptAnnotation] = "true"
				return nil
			}); err != nil {
				return err
			}
			fmt.Printf("marked %q for adoption; the operator will take ownership on the next reconcile\n", args[0])
			return nil
		},
	}
}

func newDBUserDropCommand() *cobra.Command {
	var reclaim string
	cmd := &cobra.Command{
		Use:   "drop NAME",
		Short: "Delete a DatabaseUser resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			du := &mysqlv1alpha1.DatabaseUser{}
			if err := env.Client.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: args[0]}, du); err != nil {
				return err
			}
			if reclaim != "" && du.Spec.ReclaimPolicy != reclaim {
				before := du.DeepCopy()
				du.Spec.ReclaimPolicy = reclaim
				if err := env.Client.Patch(ctx, du, client.MergeFrom(before)); err != nil {
					return err
				}
			}
			if err := env.Client.Delete(ctx, du); err != nil {
				return err
			}
			fmt.Printf("deleted databaseuser %q (reclaim: %s)\n", args[0], defaultReclaim(du.Spec.ReclaimPolicy))
			return nil
		},
	}
	cmd.Flags().StringVar(&reclaim, "reclaim", "", "set reclaim policy before delete: retain|delete")
	return cmd
}

func newDBUserDBaaSCommand() *cobra.Command {
	var (
		clusterName string
		userName    string
		secretRef   string
		generate    bool
	)
	cmd := &cobra.Command{
		Use:   "dbaas NAME --cluster CLUSTER",
		Short: "Create a constrained DBaaS admin (broad data privileges, no cluster control)",
		Long: "Scaffold a DatabaseUser with full data and schema privileges across all " +
			"databases, granted by name rather than ALL, plus revokes that carve the " +
			"system schemas (mysql.*, sys.*) out of the *.* grant. The revokes require " +
			"partial_revokes=ON on the cluster (set it via spec.mysql.parameters); " +
			"without it the tenant retains write access to mysql.* and can self-escalate.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if clusterName == "" {
				return fmt.Errorf("--cluster is required")
			}
			env, err := newEnv()
			if err != nil {
				return err
			}
			name := args[0]
			secretName, secretKey := secretRefOrDefault(secretRef, name)
			du := &mysqlv1alpha1.DatabaseUser{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
				Spec: mysqlv1alpha1.DatabaseUserSpec{
					Cluster:        mysqlv1alpha1.LocalObjectReference{Name: clusterName},
					Name:           userName,
					PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: secretName, Key: secretKey},
					// Broad static data privileges by name, never ALL on *.*: a global
					// ALL also grants every dynamic admin privilege and would put the
					// tenant on the operator's control plane.
					Grants: []mysqlv1alpha1.DatabaseUserGrant{
						{Privileges: mysqlv1alpha1.SafeDBaaSAdminPrivileges(), On: "*.*"},
					},
					// Carve the system schemas out of the *.* grant so the tenant
					// cannot write the grant tables. Needs partial_revokes=ON.
					Revokes: mysqlv1alpha1.SafeDBaaSAdminRevokes(),
				},
			}
			if generate {
				pw, err := writeGeneratedPassword(ctx, env, secretName, secretKey)
				if err != nil {
					return err
				}
				fmt.Printf("generated password (store it safely): %s\n", pw)
			}
			if err := env.Client.Create(ctx, du); err != nil {
				return fmt.Errorf("creating databaseuser: %w", err)
			}
			fmt.Printf("created DBaaS admin %q for cluster %q\n", name, clusterName)
			return nil
		},
	}
	cmd.Flags().StringVar(&clusterName, "cluster", "", "target cluster (required)")
	cmd.Flags().StringVar(&userName, "user", "", "MySQL user name (default: resource name)")
	cmd.Flags().StringVar(&secretRef, "password-secret", "", "password Secret as name/key (default: <name>-pw/password)")
	cmd.Flags().BoolVar(&generate, "generate", false, "generate a password and write it to the Secret")
	return cmd
}

// patchDatabaseUser fetches, mutates, and patches a DatabaseUser by name.
func patchDatabaseUser(
	ctx context.Context, env *plugin.Env, name string, mutate func(*mysqlv1alpha1.DatabaseUser) error,
) error {
	du := &mysqlv1alpha1.DatabaseUser{}
	if err := env.Client.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, du); err != nil {
		return err
	}
	before := du.DeepCopy()
	if err := mutate(du); err != nil {
		return err
	}
	if err := env.Client.Patch(ctx, du, client.MergeFrom(before)); err != nil {
		return err
	}
	fmt.Printf("updated databaseuser %q\n", name)
	return nil
}

// parseGrantFlags parses "PRIV[,PRIV] ON target" entries into grants. A missing
// "ON target" defaults to "*.*".
func parseGrantFlags(flags []string) ([]mysqlv1alpha1.DatabaseUserGrant, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	out := make([]mysqlv1alpha1.DatabaseUserGrant, 0, len(flags))
	for _, f := range flags {
		privPart, target := f, "*.*"
		if idx := strings.Index(strings.ToUpper(f), " ON "); idx >= 0 {
			privPart = f[:idx]
			target = strings.TrimSpace(f[idx+4:])
		}
		privs := splitCSV(privPart)
		if len(privs) == 0 {
			return nil, fmt.Errorf("grant %q has no privileges", f)
		}
		out = append(out, mysqlv1alpha1.DatabaseUserGrant{Privileges: privs, On: target})
	}
	return out, nil
}

// applyGrantDelta merges added grants and removes matching ones, keyed by target.
func applyGrantDelta(existing, add, remove []mysqlv1alpha1.DatabaseUserGrant) []mysqlv1alpha1.DatabaseUserGrant {
	byTarget := map[string]mysqlv1alpha1.DatabaseUserGrant{}
	order := []string{}
	track := func(g mysqlv1alpha1.DatabaseUserGrant) {
		if _, ok := byTarget[g.On]; !ok {
			order = append(order, g.On)
		}
		byTarget[g.On] = g
	}
	for _, g := range existing {
		track(g)
	}
	for _, g := range add {
		track(g)
	}
	for _, g := range remove {
		delete(byTarget, g.On)
	}
	out := make([]mysqlv1alpha1.DatabaseUserGrant, 0, len(byTarget))
	for _, target := range order {
		if g, ok := byTarget[target]; ok {
			out = append(out, g)
		}
	}
	return out
}

// secretRefOrDefault parses a name/key secret reference, defaulting to
// "<resource>-pw" / "password".
func secretRefOrDefault(ref, resource string) (name, key string) {
	if ref == "" {
		return resource + "-pw", "password"
	}
	if name, key, ok := strings.Cut(ref, "/"); ok {
		return name, key
	}
	return ref, "password"
}

// writeGeneratedPassword generates a random password, writes it to the Secret,
// and returns it.
func writeGeneratedPassword(ctx context.Context, env *plugin.Env, name, key string) (string, error) {
	pw, err := generatePassword()
	if err != nil {
		return "", err
	}
	if err := writePassword(ctx, env, name, key, pw); err != nil {
		return "", err
	}
	return pw, nil
}

// writePassword creates or updates the Secret key with the given password.
func writePassword(ctx context.Context, env *plugin.Env, name, key, password string) error {
	secret := &corev1.Secret{}
	err := env.Client.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, secret)
	switch {
	case apierrors.IsNotFound(err):
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace},
			Data:       map[string][]byte{key: []byte(password)},
		}
		return env.Client.Create(ctx, secret)
	case err != nil:
		return err
	default:
		before := secret.DeepCopy()
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data[key] = []byte(password)
		return env.Client.Patch(ctx, secret, client.MergeFrom(before))
	}
}

func generatePassword() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func appliedString(applied *bool) string {
	if applied == nil {
		return "-"
	}
	return strconv.FormatBool(*applied)
}

func readyReason(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == mysqlv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}

func defaultReclaim(policy string) string {
	if policy == "" {
		return "retain"
	}
	return policy
}
