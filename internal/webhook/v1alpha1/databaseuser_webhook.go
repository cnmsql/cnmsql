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

package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

var databaseuserlog = logf.Log.WithName("databaseuser-validator")

// mariadbRevokesUnsupported explains why spec.revokes cannot be honoured on
// MariaDB: it has neither MySQL's partial_revokes nor a DENY statement (DENY is
// planned for MariaDB 13.1, not yet released), so a system-schema carve-out
// against a broad grant cannot be enforced. Allowing it would silently leave the
// account over-privileged, so admission rejects it outright.
const mariadbRevokesUnsupported = "spec.revokes is not supported on MariaDB clusters: MariaDB has no " +
	"partial_revokes and no DENY statement yet (DENY is planned for MariaDB 13.1), so a system-schema " +
	"carve-out cannot be enforced and the account would remain over-privileged. Scope grants to specific " +
	"schemas (on: \"mydb.*\") instead of carving mysql.* out of a *.* grant."

// +kubebuilder:webhook:path=/validate-mysql-cnmsql-io-v1alpha1-databaseuser,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups=mysql.cnmsql.co,resources=databaseusers,verbs=create;update,versions=v1alpha1,name=vdatabaseuser-v1alpha1.cnmsql.co

// SetupDatabaseUserWebhookWithManager registers the validating webhook for
// DatabaseUser create/update. It rejects spec.revokes on MariaDB clusters, which
// cannot enforce the carve-out (see mariadbRevokesUnsupported).
func SetupDatabaseUserWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(
		"/validate-mysql-cnmsql-io-v1alpha1-databaseuser",
		&admission.Webhook{
			Handler: &DatabaseUserValidator{
				Decoder: admission.NewDecoder(mgr.GetScheme()),
				Client:  mgr.GetClient(),
			},
		},
	)
	return nil
}

// DatabaseUserValidator validates DatabaseUser create/update against the
// referenced cluster's engine flavor. It reads the Cluster to learn the flavor;
// flavor is immutable, so an admission-time read is stable.
type DatabaseUserValidator struct {
	Decoder admission.Decoder
	Client  client.Reader
}

// Handle implements admission.Handler.
func (v *DatabaseUserValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	du := &mysqlv1alpha1.DatabaseUser{}
	if err := v.Decoder.Decode(req, du); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode DatabaseUser object: %w", err))
	}

	// Only revokes diverge by flavor; nothing else to check here.
	if len(du.Spec.Revokes) == 0 {
		return admission.Allowed("")
	}

	// Determine the referenced cluster's flavor. If the cluster does not exist
	// yet (a DatabaseUser may be created before its Cluster), admit it — the
	// controller re-checks the flavor at reconcile and refuses to apply.
	cluster := &mysqlv1alpha1.Cluster{}
	err := v.Client.Get(ctx, types.NamespacedName{Namespace: du.Namespace, Name: du.Spec.Cluster.Name}, cluster)
	if apierrors.IsNotFound(err) {
		return admission.Allowed("")
	}
	if err != nil {
		return admission.Errored(500, fmt.Errorf("looking up cluster %q: %w", du.Spec.Cluster.Name, err))
	}

	if cluster.ResolvedFlavor() == mysqlv1alpha1.FlavorMariaDB {
		databaseuserlog.V(1).Info("Rejecting DatabaseUser revokes on MariaDB cluster",
			"databaseuser", req.Name, "cluster", du.Spec.Cluster.Name)
		return admission.Denied(mariadbRevokesUnsupported)
	}
	return admission.Allowed("")
}
