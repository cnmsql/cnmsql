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

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

var databaselog = logf.Log.WithName("database-validator")

// +kubebuilder:webhook:path=/validate-mysql-cnmsql-io-v1alpha1-database,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups=mysql.cnmsql.co,resources=databases,verbs=create;update,versions=v1alpha1,name=vdatabase-v1alpha1.cnmsql.co

// SetupDatabaseWebhookWithManager registers the validating webhook for
// Database create/update. It enforces Database.Validate, which confines the
// inline users' grants to the managed schema.
func SetupDatabaseWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(
		"/validate-mysql-cnmsql-io-v1alpha1-database",
		&admission.Webhook{Handler: &DatabaseValidator{Decoder: admission.NewDecoder(mgr.GetScheme())}},
	)
	return nil
}

// DatabaseValidator validates Database create/update.
type DatabaseValidator struct {
	Decoder admission.Decoder
}

// Handle implements admission.Handler.
func (v *DatabaseValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	db := &mysqlv1alpha1.Database{}
	if err := v.Decoder.Decode(req, db); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode Database object: %w", err))
	}

	// An update that leaves the spec alone (a finalizer or label change) is
	// admitted, so a Database stored before this check can still be deleted.
	// The controller refuses to apply it until the spec is fixed.
	if req.Operation == admissionv1.Update {
		old := &mysqlv1alpha1.Database{}
		if err := v.Decoder.DecodeRaw(req.OldObject, old); err != nil {
			return admission.Errored(400, fmt.Errorf("could not decode old Database object: %w", err))
		}
		if equality.Semantic.DeepEqual(old.Spec, db.Spec) {
			return admission.Allowed("")
		}
	}

	if errs := db.Validate(); len(errs) > 0 {
		databaselog.V(1).Info("Rejecting invalid Database", "database", req.Name, "errors", errs.ToAggregate().Error())
		return admission.Denied(errs.ToAggregate().Error())
	}
	return admission.Allowed("")
}
