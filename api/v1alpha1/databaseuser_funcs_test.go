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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validUser(mutate func(*DatabaseUser)) *DatabaseUser {
	u := &DatabaseUser{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant"},
		Spec: DatabaseUserSpec{
			Cluster:        LocalObjectReference{Name: "c"},
			PasswordSecret: &SecretKeySelector{Name: "s", Key: "password"},
			Grants:         []DatabaseUserGrant{{Privileges: []string{"SELECT"}, On: "app.*"}},
		},
	}
	u.SetDefaults()
	if mutate != nil {
		mutate(u)
	}
	return u
}

func TestDatabaseUserValidateOK(t *testing.T) {
	if errs := validUser(nil).Validate(); len(errs) != 0 {
		t.Fatalf("expected valid user, got %v", errs)
	}
}

func TestDatabaseUserValidateRejectsDeniedPrivilege(t *testing.T) {
	u := validUser(func(u *DatabaseUser) {
		u.Spec.Grants = []DatabaseUserGrant{{Privileges: []string{"REPLICATION_SLAVE_ADMIN"}, On: "*.*"}}
	})
	if errs := u.Validate(); len(errs) == 0 {
		t.Fatalf("expected denied-privilege rejection")
	}
}

func TestDatabaseUserValidateRejectsGlobalAll(t *testing.T) {
	for _, priv := range []string{"ALL", "all privileges", "ALL PRIVILEGES"} {
		u := validUser(func(u *DatabaseUser) {
			u.Spec.Grants = []DatabaseUserGrant{{Privileges: []string{priv}, On: "*.*"}}
		})
		if errs := u.Validate(); len(errs) == 0 {
			t.Errorf("expected ALL on *.* (%q) to be rejected", priv)
		}
	}
}

func TestDatabaseUserValidateAllowsSchemaScopedAll(t *testing.T) {
	u := validUser(func(u *DatabaseUser) {
		u.Spec.Grants = []DatabaseUserGrant{{Privileges: []string{"ALL"}, On: "app.*"}}
	})
	if errs := u.Validate(); len(errs) != 0 {
		t.Fatalf("ALL on a single schema must be allowed, got %v", errs)
	}
}

func TestDatabaseUserValidateRevokes(t *testing.T) {
	ok := validUser(func(u *DatabaseUser) {
		u.Spec.Grants = []DatabaseUserGrant{{Privileges: SafeDBaaSAdminPrivileges(), On: "*.*"}}
		u.Spec.Revokes = SafeDBaaSAdminRevokes()
	})
	if errs := ok.Validate(); len(errs) != 0 {
		t.Fatalf("safe-admin grants+revokes must validate, got %v", errs)
	}

	noTarget := validUser(func(u *DatabaseUser) {
		u.Spec.Revokes = []DatabaseUserRevoke{{Privileges: []string{"INSERT"}}}
	})
	if errs := noTarget.Validate(); len(errs) == 0 {
		t.Errorf("expected a revoke without a target to be rejected")
	}
}

func TestDatabaseUserValidateRejectsReservedAndSuperuserGrants(t *testing.T) {
	reserved := validUser(func(u *DatabaseUser) { u.Spec.Name = "cnmsql_repl" })
	if errs := reserved.Validate(); len(errs) == 0 {
		t.Errorf("expected reserved name rejection")
	}
	both := validUser(func(u *DatabaseUser) { u.Spec.Superuser = true })
	if errs := both.Validate(); len(errs) == 0 {
		t.Errorf("expected superuser+grants rejection")
	}
}
